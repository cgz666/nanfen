import sys
import os
root_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
sys.path.insert(0, root_dir)
import pickle
import schedule
import time
import requests
import pandas as pd
import shutil
from datetime import datetime
from functools import wraps
from bs4 import BeautifulSoup
import pythoncom
import win32com.client as win32
import foura_data
from core.config import settings


COOKIE_FILE_PATH = settings.resolve_path('scheduler/cookie.pkl')
BASE_URL = 'http://omms.chinatowercom.cn:9000'
LOGIN_URL = f'{BASE_URL}/layout/index.xhtml'
def retry(max_attempts=3, delay=2):
    def decorator_retry(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            attempts = 0
            while attempts < max_attempts:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    # 如果是 Cookie 失效错误，不重试，直接抛出以便上层处理
                    if "Cookie失效" in str(e):
                        raise
                    attempts += 1
                    if attempts == max_attempts:
                        raise
                    time.sleep(delay)

        return wrapper

    return decorator_retry
@retry()
def requests_post_safe(url, headers={}, data={}, cookies={}, timeout=600):
    """发送POST请求，带有重试机制"""
    return requests.post(url, headers=headers, data=data, cookies=cookies, timeout=timeout)
@retry()
def requests_get_safe(url, headers={}, params={}, cookies={}, timeout=600):
    """发送GET请求，带有重试机制"""
    return requests.get(url, headers=headers, params=params, cookies=cookies, timeout=timeout)
def get_foura_cookie():
    """从pkl文件获取cookie，如果没有或失败则抛出异常"""
    if not os.path.exists(COOKIE_FILE_PATH):
        raise FileNotFoundError(f"Cookie文件不存在: {COOKIE_FILE_PATH}")

    try:
        with open(COOKIE_FILE_PATH, 'rb') as f:
            cookies = pickle.load(f)
        if not cookies:
            raise ValueError("Cookie文件为空")
        return cookies
    except Exception as e:
        raise Exception(f"读取Cookie文件失败: {e}")
def down_file_single(url, data, path, conten_len_error=1000):
    try:
        headers = {
            'Accept': '*/*',
            'Accept-Encoding': 'gzip, deflate',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6',
            'Connection': 'keep-alive',
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'Host': 'omms.chinatowercom.cn:9000',
            'Origin': 'http://omms.chinatowercom.cn:9000',
            'Referer': url,
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36'
        }
        cookies = get_foura_cookie()

        if not cookies:
            raise ValueError("Cookie 为空，无法发起请求")

        # 第一步：获取 ViewState
        res = requests_post_safe(url, headers=headers, cookies=cookies)
        html = BeautifulSoup(res.text, 'html.parser')
        view_state_elem = html.find('input', id='javax.faces.ViewState')
        if not view_state_elem:
            raise ValueError("无法获取 javax.faces.ViewState，可能权限不足或页面结构变更")

        javax = view_state_elem['value']

        # 第二步：提交表单下载
        for key, into_data in data.items():
            into_data['javax.faces.ViewState'] = javax
            res = requests_post_safe(url, headers=headers, data=into_data, cookies=cookies)
            if 'FINAL' in key:
                # 内容长度检查
                if len(res.content) < conten_len_error:
                    raise ValueError(f"内容小于给定大小 ({len(res.content)} < {conten_len_error})")
                with open(path, "wb") as codes:
                    codes.write(res.content)
        return
    except Exception:
        raise
def clean_down_dir():
    """清理spider/down目录下所有文件，保留子目录结构"""
    down_dir = settings.resolve_path("spider/down")
    if os.path.exists(down_dir):
        for root, _, files in os.walk(down_dir):
            for file in files:
                os.remove(os.path.join(root, file))

"""
1. 爬取站址信息
"""
class Station():
    def __init__(self):
        self.data = foura_data.station
        self.URL = 'http://omms.chinatowercom.cn:9000/business/resMge/siteMge/listSite.xhtml'
        self.down_name = '站址信息'
        self.output_path = settings.resolve_path(f"spider/down/station/{self.down_name}.xlsx")

    def down(self):
        down_file_single(self.URL, self.data, self.output_path)
        df = pd.read_excel(self.output_path, dtype={'站址编码': str})
        df = df[['站址编码', '所属运营商', '站址保障等级', '区县（行政区划）']]
        df.to_excel(self.output_path, index=False)

    def main(self):
        self.down()
        print(f"下载完成: {self.output_path}")

"""
2. 爬取FSU监控 (离线)
"""
class FsuJianKong():
    def __init__(self):
        self.data = foura_data.fsu_jiankong
        self.URL = 'http://omms.chinatowercom.cn:9000/business/resMge/pwMge/fsuMge/listFsu.xhtml'
        self.output_path = settings.resolve_path('spider/down/fsu_lixian/fsu离线.xlsx')

    def down(self):
        down_file_single(self.URL, self.data, self.output_path)

    def main(self):
        self.down()
        print("FSU监控下载完成")

"""
3. 爬取历史告警Hbase
"""
class AlarmHistoryHbase():
    def __init__(self):
        self.data = foura_data.alarm_history_Hbase
        self.now = datetime.now()
        self.URL = 'http://omms.chinatowercom.cn:9000/business/resMge/alarmHisHbaseMge/listHisAlarmHbase.xhtml'
        self.output_dir = settings.resolve_path("spider\down\Hbase")

    def down(self):
        alarm_names = ['交流输入停电告警','一级低压脱离告警']
        start = self.now.replace(hour=0, minute=0, second=0, microsecond=0)
        end = datetime.now()

        # 下载第一个告警（用于创建文件）
        first_alarm = alarm_names[0]
        for key in ['1']:
            self.data[key]['queryForm:queryalarmName'] = first_alarm
            self.data[key]['queryForm:firststarttimeInputCurrentDate'] = start.strftime('%m/%Y')
            self.data[key]['queryForm:firstendtimeInputCurrentDate'] = end.strftime('%m/%Y')
            self.data[key]['queryForm:firststarttimeInputDate'] = start.strftime('%Y-%m-%d %H:%M')
            self.data[key]['queryForm:firstendtimeInputDate'] = end.strftime('%Y-%m-%d %H:%M')

        filename = "hbase.xlsx"
        path = os.path.join(self.output_dir, filename)

        try:
            down_file_single(self.URL, self.data, path)
            print(f"下载成功: {first_alarm}")
        except Exception as e:
            print(f"下载失败 {first_alarm}: {e}")
            return

        # # 下载第二个告警（追加到同一文件）
        # second_alarm = alarm_names[1]
        # for key in ['1']:
        #     self.data[key]['queryForm:queryalarmName'] = second_alarm
        # try:
        #     down_file_single(self.URL, self.data, path)
        #     print(f"下载成功: {second_alarm}")
        # except Exception as e:
        #     print(f"下载失败 {second_alarm}: {e}")

    def main(self):
        self.down()

"""
4-5. 爬取告警数据
"""
class AlarmDownloader():
    def __init__(self):
        self.data = foura_data.alarm_now
        self.URL = 'http://omms.chinatowercom.cn:9000/business/resMge/alarmMge/listAlarm.xhtml'
        # 告警名称列表
        self.alarm_names = [
            '交流输入停电告警',
            '一级低压脱离告警',
            '温度过高',
            '温度超高',
            '电池供电告警',
            '交流输入缺相告警',
            '总电压过低告警',
            '整流模块故障告警',
            '直流输出电压过低告警'
        ]

    def down(self):
        for alarm_name in self.alarm_names:
            for key in ['1', '2']:
                if key in self.data:
                    self.data[key]['queryForm:fscidText'] = alarm_name

            # 温度超高追加到温度过高文件
            if alarm_name == '温度超高':
                target_path = settings.resolve_path("spider/down/alarm_now/温度过高.xlsx")
                temp_path = settings.resolve_path("spider/down/alarm_now/temp_温度超高.xlsx")
                down_file_single(self.URL, self.data, temp_path)
                self._merge_excel(target_path, temp_path)
            else:
                output_path = settings.resolve_path(f"spider/down/alarm_now/{alarm_name}.xlsx")
                down_file_single(self.URL, self.data, output_path)
                print(f"下载完成: {alarm_name} -> {output_path}")

    def _merge_excel(self, target_path, source_path):
        """将source_path的数据追加到target_path，保持长数字串为文本格式"""
        try:
            try:
                df_source = pd.read_excel(source_path, dtype=str)
            except:
                return

            if os.path.exists(target_path):
                # 目标文件存在，读取后追加（同样作为字符串）
                df_target = pd.read_excel(target_path, dtype=str)
                df_merged = pd.concat([df_target, df_source], ignore_index=True)
            else:
                # 目标文件不存在，直接用源数据
                df_merged = df_source

            # 保存合并后的结果，使用openpyxl引擎，保持文本格式
            with pd.ExcelWriter(target_path, engine='openpyxl') as writer:
                df_merged.to_excel(writer, index=False)

                # 获取工作表，将所有列设置为文本格式
                worksheet = writer.sheets['Sheet1']
                for column in worksheet.columns:
                    for cell in column:
                        # 如果单元格值是数字字符串，确保它保持为字符串
                        if isinstance(cell.value, str) and cell.value.isdigit():
                            cell.number_format = '@'  # 文本格式

        except Exception as e:
            print(f"合并文件失败: {e}")
            raise

    def main(self):
        self.down()

"""
6. 爬取故障监控 
"""
class FaultMonitoring():
    def __init__(self):
        self.data = foura_data.fault_monitoring
        self.URL = 'http://omms.chinatowercom.cn:9000/business/resMge/faultAlarmMge/listFaultActive.xhtml'
        self.down_name = '故障监控'
        self.output_path = settings.resolve_path(f"spider/down/fault_monitoring/{self.down_name}.xls")

    def down(self):
        down_file_single(self.URL, self.data, self.output_path)

    def main(self):
        self.down()
        print(f"下载完成: {self.output_path}")

"""
7. 爬取发电工单
"""
class PowerWorkOrder():
    def __init__(self):
        self.down_name = '发电工单'
        self.pickle_path = settings.resolve_path('spider/down/power_workorder/pickle_quxin.pkl')
        self.output_path = settings.resolve_path(f"spider/down/power_workorder/{self.down_name}.xls")

    def update_session(self):
        """更新 pickle 文件（获取最新 session）"""
        res = requests.get('http://clound.gxtower.cn:3980/tt/get_session_quxin')
        with open(self.pickle_path, "wb") as file:
            file.write(res.content)

    def get_date_range(self):
        """获取当天0点到当前时间的时间范围"""
        now = datetime.now()
        begin = now.replace(hour=0, minute=0, second=0, microsecond=0)
        end = now
        return begin.strftime('%Y-%m-%d %H:%M:%S'), end.strftime('%Y-%m-%d %H:%M:%S')

    def down_core(self, begin, end, session):
        url = 'http://clound.gxtower.cn:11080/tower_manage_bms/a/tower/oil/report/exportDeviReportNew'

        data = {
            "pageNo": "1",
            "pageSize": "25",
            "city.id": "1115",
            "area.id": "",
            "stationName": "",
            "stationCode": "",
            "shareOper": "",
            "isStart": "",
            "powerOper": "",
            "number": "",
            "collectorCode": "",
            "generatePowerState": "",
            "generateOfficeName": "",
            "workProWay": "",
            "finishConfigId": "",
            "approvalOfDispatchId": "",
            "mobilePushStatus": "",
            "mobileAuditStatus": "",
            "mobilePushTimes": "",
            "safeOrderNumber": "",
            "g5Flag": "",
            "pushFlag": "",
            "settlementFlag": "",
            "ctccSysAudit": "",
            "operatorOrderNum": "",
            "auditTelecomValue": "",
            "asOper": "",
            "isMark": "",
            "beginGenerateDate": begin,
            "endGenerateDate": end
        }

        res = session.post(url=url,data=data)
        with open(self.output_path, "wb") as file:
            file.write(res.content)

    def main(self):
        self.update_session()
        begin_generate_date, end_generate_date = self.get_date_range()
        with open(self.pickle_path, 'rb') as f:
            session = pickle.load(f)
        self.down_core(begin_generate_date, end_generate_date, session)

"""
8. 处理Excel
"""
class ExcelProcess():
    def __init__(self):
        self.save_path = settings.resolve_path( "spider/down")
        self.output_path = settings.resolve_path( "spider/output")
        self.output_name1 = os.path.join(self.output_path, "应急保障信息通报.xlsx")
        self.output_name2 = os.path.join(self.output_path, "运营商高等级站点告警通报.xlsx")
        self.model_path1 = os.path.join(self.output_path, "模板1.xlsx")
        self.model_path2 = os.path.join(self.output_path, "模板2.xlsx")
        self.down_name1 = 'station'
        self.down_name2 = 'fsu_lixian'
        self.down_name3 = 'hbase'
        self.down_name4 = 'alarm_now'
        self.down_name5 = 'fault_monitoring'
        self.down_name6 = 'power_workorder'
        self.file_name1 = settings.resolve_path(f"{self.save_path}/{self.down_name1}/站址信息.xlsx")
        self.file_name2 = settings.resolve_path(f"{self.save_path}/{self.down_name2}/fsu离线.xlsx")
        self.file_name3 = settings.resolve_path(f"{self.save_path}/{self.down_name3}/hbase.xlsx")
        self.file_name4 = settings.resolve_path(f"{self.save_path}/{self.down_name4}/交流输入停电告警.xlsx")
        self.file_name5 = settings.resolve_path(f"{self.save_path}/{self.down_name4}/一级低压脱离告警.xlsx")
        self.file_name6 = settings.resolve_path(f"{self.save_path}/{self.down_name5}/故障监控.xls")
        self.file_name7 = settings.resolve_path(f"{self.save_path}/{self.down_name6}/发电工单.xls")
        self.file_name8 = settings.resolve_path(f"{self.save_path}/{self.down_name4}/温度过高.xlsx")
        self.file_name9 = settings.resolve_path(f"{self.save_path}/{self.down_name4}/电池供电告警.xlsx")
        self.file_name10 = settings.resolve_path(f"{self.save_path}/{self.down_name4}/交流输入缺相告警.xlsx")
        self.file_name11 = settings.resolve_path(f"{self.save_path}/{self.down_name4}/总电压过低告警.xlsx")
        self.file_name12 = settings.resolve_path(f"{self.save_path}/{self.down_name4}/整流模块故障告警.xlsx")
        self.file_name13 = settings.resolve_path(f"{self.save_path}/{self.down_name4}/直流输出电压过低告警.xlsx")

    def excel_process1(self):
        """处理Excel文件，将数据文件内容复制到主表文件中"""
        print('开始处理Excel文件...')
        pythoncom.CoInitialize()
        try:
            xl = win32.gencache.EnsureDispatch('Excel.Application')
            xl.Visible = False
            xl.DisplayAlerts = False
            workbook_main = xl.Workbooks.Open(self.model_path1)

            # 1. 站址信息
            workbook_data = xl.Workbooks.Open(self.file_name1)
            sheet_data = workbook_data.Sheets(1)
            sheet_main = workbook_main.Sheets('站点管理')
            last_row = sheet_data.Cells(sheet_data.Rows.Count, 1).End(win32.constants.xlUp).Row
            source_range = sheet_data.Range(f'A1:D{last_row}')

            last_clear_row = sheet_main.UsedRange.Rows.Count
            if last_clear_row > 1:
                sheet_main.Range(f"A1:D{last_clear_row}").ClearContents()

            source_range.Copy()
            target_range = sheet_main.Range('A1')
            target_range.PasteSpecial(Paste=win32.constants.xlPasteValues)
            xl.CutCopyMode = False
            workbook_data.Close(SaveChanges=False)

            # 2. fsu离线
            workbook_data = xl.Workbooks.Open(self.file_name2)
            sheet_data = workbook_data.Sheets(1)
            sheet_main = workbook_main.Sheets('FSU离线')
            last_row = sheet_data.Cells(sheet_data.Rows.Count, 1).End(win32.constants.xlUp).Row
            source_range = sheet_data.Range(f'A1:CI{last_row}')

            last_clear_row = sheet_main.UsedRange.Rows.Count
            if last_clear_row > 1:
                sheet_main.Range(f"A1:CI{last_clear_row}").ClearContents()

            source_range.Copy()
            target_range = sheet_main.Range('A1')
            target_range.PasteSpecial(Paste=win32.constants.xlPasteValues)
            xl.CutCopyMode = False
            if last_row > 1:
                sheet_main.Range('CJ2').AutoFill(sheet_main.Range(f'CJ2:CJ{last_row}'), win32.constants.xlFillDefault)
                sheet_main.Range('CK2').AutoFill(sheet_main.Range(f'CK2:CK{last_row}'), win32.constants.xlFillDefault)
            workbook_data.Close(SaveChanges=False)

            # 3. hbase
            workbook_data = xl.Workbooks.Open(self.file_name3)
            sheet_data = workbook_data.Sheets(1)
            sheet_main = workbook_main.Sheets('历史告警Hbase查询')
            last_row = sheet_data.Cells(sheet_data.Rows.Count, 1).End(win32.constants.xlUp).Row
            source_range = sheet_data.Range(f'A1:AV{last_row}')

            last_clear_row = sheet_main.UsedRange.Rows.Count
            if last_clear_row > 1:
                sheet_main.Range(f"A1:AV{last_clear_row}").ClearContents()

            source_range.Copy()
            target_range = sheet_main.Range('A1')
            target_range.PasteSpecial(Paste=win32.constants.xlPasteValues)
            xl.CutCopyMode = False

            if last_row > 1:
                sheet_main.Range('AW2').AutoFill(sheet_main.Range(f'AW2:AW{last_row}'), win32.constants.xlFillDefault)
                sheet_main.Range('AX2').AutoFill(sheet_main.Range(f'AX2:AX{last_row}'), win32.constants.xlFillDefault)
                sheet_main.Range('AY2').AutoFill(sheet_main.Range(f'AY2:AY{last_row}'), win32.constants.xlFillDefault)
                sheet_main.Range('AZ2').AutoFill(sheet_main.Range(f'AZ2:AZ{last_row}'), win32.constants.xlFillDefault)
            workbook_data.Close(SaveChanges=False)

            # 4. alarm_now交流输入停电告警
            workbook_data = xl.Workbooks.Open(self.file_name4)
            sheet_data = workbook_data.Sheets(1)
            sheet_main = workbook_main.Sheets('交流输入停电')
            last_row = sheet_data.Cells(sheet_data.Rows.Count, 1).End(win32.constants.xlUp).Row
            source_range = sheet_data.Range(f'A1:BJ{last_row}')
            last_clear_row = sheet_main.UsedRange.Rows.Count
            if last_clear_row > 1:
                sheet_main.Range(f"A1:BJ{last_clear_row}").ClearContents()
            source_range.Copy()
            target_range = sheet_main.Range('A1')
            target_range.PasteSpecial(Paste=win32.constants.xlPasteValues)
            xl.CutCopyMode = False
            if last_row > 1:
                sheet_main.Range('BK2').AutoFill(sheet_main.Range(f'BK2:BK{last_row}'), win32.constants.xlFillDefault)
                sheet_main.Range('BL2').AutoFill(sheet_main.Range(f'BL2:BL{last_row}'), win32.constants.xlFillDefault)
            workbook_data.Close(SaveChanges=False)

            # 5. alarm_now一级低压脱离告警
            workbook_data = xl.Workbooks.Open(self.file_name5)
            sheet_data = workbook_data.Sheets(1)
            sheet_main = workbook_main.Sheets('退服')
            last_row = sheet_data.Cells(sheet_data.Rows.Count, 1).End(win32.constants.xlUp).Row
            source_range = sheet_data.Range(f'A1:BJ{last_row}')
            last_clear_row = sheet_main.UsedRange.Rows.Count
            if last_clear_row > 1:
                sheet_main.Range(f"A1:BJ{last_clear_row}").ClearContents()
            source_range.Copy()
            target_range = sheet_main.Range('A1')
            target_range.PasteSpecial(Paste=win32.constants.xlPasteValues)
            xl.CutCopyMode = False
            if last_row > 1:
                sheet_main.Range('BK2').AutoFill(sheet_main.Range(f'BK2:BK{last_row}'), win32.constants.xlFillDefault)
                sheet_main.Range('BL2').AutoFill(sheet_main.Range(f'BL2:BL{last_row}'), win32.constants.xlFillDefault)
            workbook_data.Close(SaveChanges=False)

            # 6. 故障监控
            workbook_data = xl.Workbooks.Open(self.file_name6)
            sheet_data = workbook_data.Sheets(1)
            sheet_main = workbook_main.Sheets('疑似退服')
            last_row = sheet_data.Cells(sheet_data.Rows.Count, 1).End(win32.constants.xlUp).Row
            source_range = sheet_data.Range(f'A1:S{last_row}')
            last_clear_row = sheet_main.UsedRange.Rows.Count
            if last_clear_row > 1:
                sheet_main.Range(f"A1:S{last_clear_row}").ClearContents()
            source_range.Copy()
            target_range = sheet_main.Range('A1')
            target_range.PasteSpecial(Paste=win32.constants.xlPasteValues)
            xl.CutCopyMode = False
            if last_row > 1:
                sheet_main.Range('T2').AutoFill(sheet_main.Range(f'T2:T{last_row}'), win32.constants.xlFillDefault)
                sheet_main.Range('U2').AutoFill(sheet_main.Range(f'U2:U{last_row}'), win32.constants.xlFillDefault)
            workbook_data.Close(SaveChanges=False)

            # 7. 发电工单
            workbook_data = xl.Workbooks.Open(self.file_name7)
            sheet_data = workbook_data.Sheets(1)
            sheet_main = workbook_main.Sheets('发电数')
            last_row = sheet_data.Cells(sheet_data.Rows.Count, 1).End(win32.constants.xlUp).Row
            source_range = sheet_data.Range(f'A3:BH{last_row}')
            last_clear_row = sheet_main.UsedRange.Rows.Count
            if last_clear_row > 1:
                sheet_main.Range(f"A2:BH{last_clear_row}").ClearContents()
            source_range.Copy()
            target_range = sheet_main.Range('A2')
            target_range.PasteSpecial(Paste=win32.constants.xlPasteValues)
            xl.CutCopyMode = False
            workbook_data.Close(SaveChanges=False)

            workbook_main.SaveAs(self.output_name1)
            workbook_main.Close()
            xl.Quit()
            print('已全部完成')
        except Exception as e:
            print(f"处理Excel时出错: {e}")
            raise
        finally:
            pythoncom.CoUninitialize()

    def excel_process2(self):
        """处理Excel文件，将数据文件内容复制到主表文件中"""
        print('开始处理Excel文件...')
        pythoncom.CoInitialize()
        try:
            xl = win32.gencache.EnsureDispatch('Excel.Application')
            xl.Visible = False
            xl.DisplayAlerts = False
            workbook_main = xl.Workbooks.Open(self.model_path2)

            # 1. 一级低压脱离
            workbook_data = xl.Workbooks.Open(self.file_name5)
            sheet_data = workbook_data.Sheets(1)
            sheet_main = workbook_main.Sheets('一级低压脱离')
            last_row = sheet_data.Cells(sheet_data.Rows.Count, 1).End(win32.constants.xlUp).Row
            source_range = sheet_data.Range(f'A1:BJ{last_row}')

            last_clear_row = sheet_main.UsedRange.Rows.Count
            if last_clear_row > 1:
                sheet_main.Range(f"A1:BJ{last_clear_row}").ClearContents()

            source_range.Copy()
            target_range = sheet_main.Range('A1')
            target_range.PasteSpecial(Paste=win32.constants.xlPasteValues)
            xl.CutCopyMode = False
            workbook_data.Close(SaveChanges=False)

            # 2. 交流输入停电
            workbook_data = xl.Workbooks.Open(self.file_name4)
            sheet_data = workbook_data.Sheets(1)
            sheet_main = workbook_main.Sheets('交流输入停电')
            last_row = sheet_data.Cells(sheet_data.Rows.Count, 1).End(win32.constants.xlUp).Row
            source_range = sheet_data.Range(f'A1:BJ{last_row}')

            last_clear_row = sheet_main.UsedRange.Rows.Count
            if last_clear_row > 1:
                sheet_main.Range(f"A1:BJ{last_clear_row}").ClearContents()

            source_range.Copy()
            target_range = sheet_main.Range('A1')
            target_range.PasteSpecial(Paste=win32.constants.xlPasteValues)
            xl.CutCopyMode = False
            workbook_data.Close(SaveChanges=False)

            # 3. FSU离线
            workbook_data = xl.Workbooks.Open(self.file_name2)
            sheet_data = workbook_data.Sheets(1)
            sheet_main = workbook_main.Sheets('FSU离线')
            last_row = sheet_data.Cells(sheet_data.Rows.Count, 1).End(win32.constants.xlUp).Row
            source_range = sheet_data.Range(f'A1:CI{last_row}')

            last_clear_row = sheet_main.UsedRange.Rows.Count
            if last_clear_row > 1:
                sheet_main.Range(f"A1:CI{last_clear_row}").ClearContents()

            source_range.Copy()
            target_range = sheet_main.Range('A1')
            target_range.PasteSpecial(Paste=win32.constants.xlPasteValues)
            xl.CutCopyMode = False
            workbook_data.Close(SaveChanges=False)

            # 4. 交流输入缺相告警
            workbook_data = xl.Workbooks.Open(self.file_name10)
            sheet_data = workbook_data.Sheets(1)
            sheet_main = workbook_main.Sheets('交流输入缺相告警')
            last_row = sheet_data.Cells(sheet_data.Rows.Count, 1).End(win32.constants.xlUp).Row
            source_range = sheet_data.Range(f'A1:BJ{last_row}')
            last_clear_row = sheet_main.UsedRange.Rows.Count
            if last_clear_row > 1:
                sheet_main.Range(f"A1:BJ{last_clear_row}").ClearContents()
            source_range.Copy()
            target_range = sheet_main.Range('A1')
            target_range.PasteSpecial(Paste=win32.constants.xlPasteValues)
            xl.CutCopyMode = False
            workbook_data.Close(SaveChanges=False)

            # 5. 总电压过低
            workbook_data = xl.Workbooks.Open(self.file_name5)
            sheet_data = workbook_data.Sheets(1)
            sheet_main = workbook_main.Sheets('总电压过低')
            last_row = sheet_data.Cells(sheet_data.Rows.Count, 1).End(win32.constants.xlUp).Row
            source_range = sheet_data.Range(f'A1:BJ{last_row}')
            last_clear_row = sheet_main.UsedRange.Rows.Count
            if last_clear_row > 1:
                sheet_main.Range(f"A1:BJ{last_clear_row}").ClearContents()
            source_range.Copy()
            target_range = sheet_main.Range('A1')
            target_range.PasteSpecial(Paste=win32.constants.xlPasteValues)
            xl.CutCopyMode = False
            workbook_data.Close(SaveChanges=False)

            # 6. 温度过高
            workbook_data = xl.Workbooks.Open(self.file_name8)
            sheet_data = workbook_data.Sheets(1)
            sheet_main = workbook_main.Sheets('温度过高告警')
            last_row = sheet_data.Cells(sheet_data.Rows.Count, 1).End(win32.constants.xlUp).Row
            source_range = sheet_data.Range(f'A1:BJ{last_row}')
            last_clear_row = sheet_main.UsedRange.Rows.Count
            if last_clear_row > 1:
                sheet_main.Range(f"A1:BJ{last_clear_row}").ClearContents()
            source_range.Copy()
            target_range = sheet_main.Range('A1')
            target_range.PasteSpecial(Paste=win32.constants.xlPasteValues)
            xl.CutCopyMode = False
            workbook_data.Close(SaveChanges=False)

            # 7.发电工单
            workbook_data = xl.Workbooks.Open(self.file_name7)
            sheet_data = workbook_data.Sheets(1)
            sheet_main = workbook_main.Sheets('取信系统_发电数')
            last_row = sheet_data.Cells(sheet_data.Rows.Count, 1).End(win32.constants.xlUp).Row
            source_range = sheet_data.Range(f'A3:BH{last_row}')
            last_clear_row = sheet_main.UsedRange.Rows.Count
            if last_clear_row > 1:
                sheet_main.Range(f"A3:BH{last_clear_row}").ClearContents()
            source_range.Copy()
            target_range = sheet_main.Range('A3')
            target_range.PasteSpecial(Paste=win32.constants.xlPasteValues)
            xl.CutCopyMode = False
            workbook_data.Close(SaveChanges=False)

            # 8.整流模块故障告警
            workbook_data = xl.Workbooks.Open(self.file_name12)
            sheet_data = workbook_data.Sheets(1)
            sheet_main = workbook_main.Sheets('整流模块故障告警')
            last_row = sheet_data.Cells(sheet_data.Rows.Count, 1).End(win32.constants.xlUp).Row
            source_range = sheet_data.Range(f'A1:BJ{last_row}')
            last_clear_row = sheet_main.UsedRange.Rows.Count
            if last_clear_row > 1:
                sheet_main.Range(f"A1:BJ{last_clear_row}").ClearContents()
            source_range.Copy()
            target_range = sheet_main.Range('A1')
            target_range.PasteSpecial(Paste=win32.constants.xlPasteValues)
            xl.CutCopyMode = False
            workbook_data.Close(SaveChanges=False)

            # 9.电池供电告警
            workbook_data = xl.Workbooks.Open(self.file_name9)
            sheet_data = workbook_data.Sheets(1)
            sheet_main = workbook_main.Sheets('电池供电告警')
            last_row = sheet_data.Cells(sheet_data.Rows.Count, 1).End(win32.constants.xlUp).Row
            source_range = sheet_data.Range(f'A1:BJ{last_row}')
            last_clear_row = sheet_main.UsedRange.Rows.Count
            if last_clear_row > 1:
                sheet_main.Range(f"A1:BJ{last_clear_row}").ClearContents()
            source_range.Copy()
            target_range = sheet_main.Range('A1')
            target_range.PasteSpecial(Paste=win32.constants.xlPasteValues)
            xl.CutCopyMode = False
            workbook_data.Close(SaveChanges=False)

            # 10.直流输出电压过低告警
            workbook_data = xl.Workbooks.Open(self.file_name13)
            sheet_data = workbook_data.Sheets(1)
            sheet_main = workbook_main.Sheets('直流输出电压过低告警')
            last_row = sheet_data.Cells(sheet_data.Rows.Count, 1).End(win32.constants.xlUp).Row
            source_range = sheet_data.Range(f'A1:BJ{last_row}')
            last_clear_row = sheet_main.UsedRange.Rows.Count
            if last_clear_row > 1:
                sheet_main.Range(f"A1:BJ{last_clear_row}").ClearContents()
            source_range.Copy()
            target_range = sheet_main.Range('A1')
            target_range.PasteSpecial(Paste=win32.constants.xlPasteValues)
            xl.CutCopyMode = False
            workbook_data.Close(SaveChanges=False)

            # 筛选联通高等级站址_匹配告警：F-N列有有效数据的行（排除#N/A）
            sheet_filter = workbook_main.Sheets('联通高等级站址_匹配告警')
            last_row = sheet_filter.Cells(sheet_filter.Rows.Count, 1).End(win32.constants.xlUp).Row

            sheet_filter.Rows.Hidden = False  # 先显示所有

            for row in range(2, last_row + 1):
                has_data = False
                for col in range(6, 15):  # F=6 到 N=14
                    cell_text = sheet_filter.Cells(row, col).Text
                    if cell_text and '#N/A' not in cell_text and cell_text.strip() != '':
                        has_data = True
                        break
                sheet_filter.Rows(row).Hidden = not has_data

            workbook_main.SaveAs(self.output_name2)
            workbook_main.Close()
            xl.Quit()
            print('已全部完成')
        except Exception as e:
            print(f"处理Excel时出错: {e}")
            raise
        finally:
            pythoncom.CoUninitialize()


# 替换原有main函数
def full_task():
    try:
        clean_down_dir()
        Station().main()
        FsuJianKong().main()
        AlarmHistoryHbase().main()
        AlarmDownloader().main()
        FaultMonitoring().main()
        PowerWorkOrder().main()
        ExcelProcess().excel_process1()
        ExcelProcess().excel_process2()
    except Exception as e:
        print(f"任务失败: {e}")

if __name__ == '__main__':
    full_task()
    schedule.every(1).hours.do(full_task)
    while True:
        schedule.run_pending()
        time.sleep(60)
