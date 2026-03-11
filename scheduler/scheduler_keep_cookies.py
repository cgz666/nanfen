import os
import time
import pickle
import datetime
import threading
import schedule
import requests
import tkinter as tk
from tkinter import simpledialog, messagebox
from bs4 import BeautifulSoup

PKL_FILE = os.path.join(os.path.dirname(__file__), 'cookie.pkl')

# 保活检测 URL (必须是登录后才能访问的页面)
KEEP_ALIVE_URL = 'http://omms.chinatowercom.cn:9000/layout/index.xhtml'

# 请求头模板
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
}

class SimpleCookieKeeper:
    def __init__(self):
        self.cookies_dict = {}

    def load_cookies(self):
        """从 pkl 加载"""
        if os.path.exists(PKL_FILE):
            try:
                with open(PKL_FILE, 'rb') as f:
                    self.cookies_dict = pickle.load(f)
                return True
            except:
                return False
        return False

    def save_cookies(self, cookies_dict):
        """保存到 pkl"""
        try:
            with open(PKL_FILE, 'wb') as f:
                pickle.dump(cookies_dict, f)
            self.cookies_dict = cookies_dict
            print(f"[{datetime.datetime.now()}] ✅ Cookie 已更新保存。")
            return True
        except Exception as e:
            print(f"保存失败: {e}")
            return False

    def ask_user_input(self):
        """弹窗获取用户输入"""
        root = tk.Tk()
        root.withdraw()
        root.attributes('-topmost', True)

        while True:
            cookie_str = simpledialog.askstring(
                "Cookie 失效 - 请更新",
                "检测到 Cookie 失效或不存在。\n\n"
                "请复制浏览器 F12 Network 中的 Cookie 字符串粘贴至此：",
                parent=root
            )

            if cookie_str is None:
                root.destroy()
                print("用户取消输入，保活暂停。")
                return False

            cookie_str = cookie_str.strip()
            if not cookie_str:
                messagebox.showwarning("提示", "输入不能为空", parent=root)
                continue

            # 解析
            new_cookies = {}
            for item in cookie_str.split(';'):
                if '=' in item:
                    k, v = item.split('=', 1)
                    new_cookies[k.strip()] = v.strip()

            if 'JSESSIONID' not in new_cookies:
                messagebox.showerror("错误", "未找到 JSESSIONID，请检查复制内容", parent=root)
                continue

            root.destroy()
            return self.save_cookies(new_cookies)

    def check_status(self):
        """检查 Cookie 是否有效"""
        if not self.cookies_dict and not self.load_cookies():
            print("未找到本地 Cookie，请求用户输入...")
            return self.ask_user_input()

        # 构造请求
        headers = HEADERS.copy()
        c_str = '; '.join([f"{k}={v}" for k, v in self.cookies_dict.items()])
        headers['Cookie'] = c_str

        try:
            res = requests.get(KEEP_ALIVE_URL, headers=headers, timeout=15, allow_redirects=True)

            if res.status_code == 200:
                soup = BeautifulSoup(res.text, 'html.parser')
                if soup.find('input', {'id': 'javax.faces.ViewState'}):
                    if not soup.find('input', {'class': 'login_btn'}) and 'login' not in res.url.lower():
                        print(f"[{datetime.datetime.now()}] 保活成功")
                        return True

            # 其他情况视为失效
            raise Exception(f"状态异常: {res.status_code} 或 页面特征不符")

        except Exception as e:
            print(f"[{datetime.datetime.now()}] 🔴 保活失败: {e}")
            return self.ask_user_input()


def run_scheduler():
    keeper = SimpleCookieKeeper()

    def job():
        keeper.check_status()

    # 首次运行立即检查
    job()

    schedule.every(5).minutes.do(job)

    while True:
        schedule.run_pending()
        time.sleep(1)


if __name__ == "__main__":
    try:
        run_scheduler()
    except KeyboardInterrupt:
        print("\n服务已停止")