# scheduler/scheduler_main.py
import schedule
import time
import threading
from datetime import datetime
import random
from core.utils.send_ding_msg import dingmsg
from core.task_logger import log_task_execution
#################       爬虫          ################################################
from spider.script.down_baobiao_system.down_baobiao_system import BaoBiaoSystem
from spider.script.down_foura import down_yitihua_order,foura_spider_universal
from spider.script.down_nenghao.down_nenghao import down_equiment_consitution,down_lixian as down_equiment_lixian
################        操作          #################################################
from scheduler.other_task import task_2359,task_7,task_month_1
################        信息          #################################################
def run_task_in_thread(task_func, task_name):
    """在独立线程中运行任务并记录日志"""
    def wrapper():
        try:
            log_task_execution(task_name, task_func)
        except Exception as e:
            print(f'{task_name}: {e}')
            try:
                msg = f'计划任务【{task_name}】报错：{e}'
                dingmsg().text_at(dingmsg().BUG, msg, [18076339136,15177191882], ['陈桂志','谢彭聪'])
            except:
                pass

    thread = threading.Thread(target=wrapper, daemon=True)
    thread.start()
    return thread

def schedule_loop():
    """主调度循环"""
    print("循环开始")
    schedule.every(5).minutes.do(run_task_in_thread,lambda: foura_spider_universal.FsuJianKong().down_5min(), "FSU监控-5分钟")

    # ==================== 每6小时执行 ====================
    schedule.every(6).hours.do(run_task_in_thread,lambda: foura_spider_universal.AlarmHistoryHbase().main(), "历史告警Hbase")

    # ==================== 每小时执行（整点） ====================
    schedule.every().hour.at(":00").do(run_task_in_thread,lambda: foura_spider_universal.FsuJianKong().down(), "FSU监控下载(整点)")

    # ==================== 每小时执行（半点） ====================
    schedule.every().hour.at(":30").do(run_task_in_thread,lambda: foura_spider_universal.FsuJianKong().down(), "FSU监控下载(半点)")

    # ==================== 每天执行（特定时间） ====================
    # 0:00
    schedule.every().day.at("00:00").do(run_task_in_thread,lambda: foura_spider_universal.StationAlias().main(), "站址别名更新")

    # 1:00
    schedule.every().day.at("01:00").do(run_task_in_thread,lambda: foura_spider_universal.FaultMonitoring().main(), "故障监控下载")

    # 7:00
    schedule.every().day.at("07:00").do(run_task_in_thread, task_7, "导出每日fsu离线情况")
    schedule.every().day.at("07:00").do(run_task_in_thread, month_3, "导入基站负载电流数据")

    # 7:40
    schedule.every().day.at("07:40").do(run_task_in_thread,lambda: foura_spider_universal.FsuChaXun().main(), "FSU查询")
    schedule.every().day.at("07:40").do(run_task_in_thread,BaoBiaoSystem().main, "报表系统下载")

    # 8:00
    schedule.every().day.at("08:00").do(run_task_in_thread,lambda: down_yitihua_order.YiTiHuaOrder().main(), "一体化工单下载")

    # 13:40
    schedule.every().day.at("13:40").do(run_task_in_thread,lambda: foura_spider_universal.FsuChaXun().main(), "FSU查询(下午)")

    # 16:40
    schedule.every().day.at("16:40").do(run_task_in_thread,lambda: foura_spider_universal.FsuChaXun().main(), "FSU查询(傍晚)")

    # 23:54
    schedule.every().day.at("23:54").do(run_task_in_thread,lambda: (down_equiment_consitution(), down_equiment_lixian()), "设备构成与离线")


    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == "__main__":
    schedule_loop()

