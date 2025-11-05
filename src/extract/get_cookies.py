from playwright.sync_api import sync_playwright, Page, BrowserContext
from loguru import logger
import time
import json
from typing import Optional, Dict, List
from dataclasses import dataclass
import base64
from typing import Optional
import os
import sys
current_dir = os.path.dirname(os.path.abspath(__file__))
project_dir = os.path.dirname(os.path.dirname(current_dir))
sys.path.append(project_dir)
os.chdir(project_dir)
print((project_dir))
from src.utils.solve_captchas import get_similar_objects_positions
from src.utils.set_proxy import get_proxy, reset_proxy
import time
import requests
import yaml
CONFIG_PATH = "./configs/secrets.yaml"
@dataclass
class ProxyConfig:
    """Cấu hình proxy - sẵn sàng cho tương lai"""
    server: str
    username: Optional[str] = None
    password: Optional[str] = None
    link_request_proxy: Optional[str] = None

    def to_dict(self) -> Dict:
        proxy = {"server": "http://"+self.server}
        if self.username:
            proxy["username"] = self.username
        if self.password:
            proxy["password"] = self.password
        return proxy
    
    def build_proxy_url(self) -> str:
        """Xây dựng proxy URL cho requests/httpx"""
        parts = self.server.split(":")
        ip, port= parts
        return f"http://{self.username}:{self.password}@{ip}:{port}"
    

    def check_proxy(self) -> bool:
        proxy_url = self.build_proxy_url()
        """Kiểm tra proxy có hoạt động không và log ra IP"""
        for i in range(3):
            try:
                with requests.Session() as session:
                    session.proxies = {
                        "http": proxy_url,
                        "https": proxy_url
                    }
                    resp = session.get("https://api.ipify.org?format=json", timeout=20)
                    if resp.status_code == 200:
                        ip = resp.json().get("ip")
                        logger.info(f"Proxy hoạt động, IP: {ip}")
                        return True
            except Exception as e:
                logger.warning(f"Lỗi khi kiểm tra proxy (lần {i+1}/{3}): {e}")
                time.sleep(2.5)
        return False


    def reset_proxy(self)->bool:
        for let_try in range(10):
            try:
                resp = requests.get(self.link_request_proxy, timeout=10)
                r = resp.json()
                if "error" not in r:
                    logger.info(f">>Proxy reset thành công: {r}")
                    time.sleep(2)
                    
                    if self.check_proxy():
                        logger.info(f">>=Proxy hoạt động tốt sau khi reset")
                        break
                    else:
                        logger.warning(f">>=Proxy không hoạt động, thử lại...")
                        time.sleep(5)
                        continue
                else:
                    logger.warning(f">>Lỗi reset proxy: {r}")
                    error = r.get("error", "")
                    time_sleep = int(error.split(" ")[-2]) + 2
                    time.sleep(time_sleep)
                
            except Exception as e:
                logger.error(f">>Exception khi reset proxy: {e}")
                time.sleep(62)

@dataclass
class LoginCredentials:
    """Thông tin đăng nhập"""
    username: str
    password: str


class CaptchaDetector:
    """Class chuyên kiểm tra captcha (cả dạng cũ và TUXModal mới của TikTok)"""

    CAPTCHA_SELECTORS = [
        # Các dạng cũ
        'iframe[id*="captcha"]',
        '[class*="captcha"]',
        '#captcha-verify-image',
        '.captcha_verify_container',
        'div[id*="captcha"]',

        # Dạng mới (TikTok TUXModal)
        '.TUXModal.captcha-verify-container',
        '#captcha-verify-container-main-page',
        'button#captcha_refresh_button',
        'button#captcha_switch_button',
        'button#captcha_close_button',
    ]

    CAPTCHA_TEXT_PATTERNS = [
        r"verify|captcha",
        r"Select\s+\d+\s+objects",
        r"same\s+shape",
        r"Confirm",
    ]

    @staticmethod
    def detect(page: Page) -> bool:
        """Kiểm tra xem có captcha hiển thị trên trang hay không"""
        try:
            # Kiểm tra theo selector (HTML element)
            for selector in CaptchaDetector.CAPTCHA_SELECTORS:
                loc = page.locator(selector)
                if loc.count() > 0:
                    logger.warning(f"Phát hiện captcha (selector: {selector})")
                    return True

            # Kiểm tra theo text hiển thị
            for pattern in CaptchaDetector.CAPTCHA_TEXT_PATTERNS:
                loc = page.locator(f"text=/{pattern}/i")
                if loc.count() > 0:
                    logger.warning(f"Phát hiện captcha (text: {pattern})")
                    return True

            return False

        except Exception as e:
            logger.error(f"Lỗi khi kiểm tra captcha: {e}")
            return False


class PageHelper:
    """Helper functions cho page operations"""
    
    @staticmethod
    def wait_for_ready(page: Page, timeout: int = 30000) -> bool:
        """Đợi trang load hoàn toàn"""
        try:
            page.wait_for_load_state('networkidle', timeout=timeout)
            logger.success("✅ Trang đã load hoàn toàn")
            return True
        except Exception as e:
            logger.warning(f"⚠️ Timeout khi đợi load: {e}")
            return False
    
    @staticmethod
    def safe_sleep(seconds: float):
        """Sleep với log"""
        time.sleep(seconds)


class TikTokLoginFlow:
    """Class xử lý toàn bộ luồng đăng nhập TikTok"""
    
    def __init__(self, page: Page):
        self.page = page
    
    def click_main_login_button(self) -> bool:
        """Bước 1: Click nút Login chính"""
        try:
            logger.info(" Đang tìm nút Login chính...")
            PageHelper.safe_sleep(2)
            
            js_code = """
            () => {
                return new Promise((resolve) => {
                    let attempts = 0;
                    const maxAttempts = 20;
                    
                    const findAndClick = () => {
                        // Tìm theo ID
                        let loginBtn = document.querySelector('#top-right-action-bar-login-button');
                        
                        // Tìm theo class
                        if (!loginBtn) {
                            loginBtn = document.querySelector('.css-1tvowuz-5e6d46e3--StyledPrimaryLoginButton');
                        }
                        
                        // Tìm theo text
                        if (!loginBtn) {
                            let buttons = document.querySelectorAll('button');
                            for (let btn of buttons) {
                                if (btn.textContent.includes('Log in')) {
                                    loginBtn = btn;
                                    break;
                                }
                            }
                        }
                        
                        if (loginBtn && loginBtn.offsetParent !== null) {
                            loginBtn.click();
                            resolve(true);
                            return;
                        }
                        
                        attempts++;
                        if (attempts < maxAttempts) {
                            setTimeout(findAndClick, 500);
                        } else {
                            resolve(false);
                        }
                    };
                    
                    findAndClick();
                });
            }
            """
            
            result = self.page.evaluate(js_code)
            
            if result:
                logger.success("Đã click nút Login chính")
                return True
            else:
                logger.error("Không tìm thấy nút Login chính")
                return False
                
        except Exception as e:
            logger.error(f"Lỗi khi click login chính: {e}")
            return False
    
    def select_phone_email_option(self) -> bool:
        """Bước 2: Chọn option 'Use phone / email / username'"""
        try:
            logger.info("Đang chọn 'Use phone / email / username'...")
            PageHelper.safe_sleep(3)
            
            js_code = """
            () => {
                return new Promise((resolve) => {
                    let attempts = 0;
                    const maxAttempts = 20;
                    
                    const findAndClick = () => {
                        // Tìm theo data-e2e
                        let options = document.querySelectorAll('[data-e2e="channel-item"]');
                        
                        for (let option of options) {
                            const text = option.textContent.toLowerCase();
                            if (text.includes('phone') || text.includes('email') || text.includes('username')) {
                                if (option.offsetParent !== null) {
                                    option.click();
                                    resolve(true);
                                    return;
                                }
                            }
                        }
                        
                        // Tìm theo role link
                        let links = document.querySelectorAll('div[role="link"]');
                        for (let link of links) {
                            const text = link.textContent.toLowerCase();
                            if (text.includes('phone') || text.includes('email') || text.includes('username')) {
                                if (link.offsetParent !== null) {
                                    link.click();
                                    resolve(true);
                                    return;
                                }
                            }
                        }
                        
                        attempts++;
                        if (attempts < maxAttempts) {
                            setTimeout(findAndClick, 500);
                        } else {
                            resolve(false);
                        }
                    };
                    
                    findAndClick();
                });
            }
            """
            
            result = self.page.evaluate(js_code)
            
            if result:
                logger.success("Đã chọn 'Use phone / email / username'")
                return True
            else:
                logger.error("Không tìm thấy option")
                return False
                
        except Exception as e:
            logger.error(f"Lỗi khi chọn option: {e}")
            return False
    
    def select_email_login_option(self) -> bool:
        """Bước 3: Click 'Đăng nhập bằng email hoặc tên người dùng'"""
        try:
            logger.info("Đang chọn 'Đăng nhập bằng email'...")
            PageHelper.safe_sleep(2)
            
            js_code = """
            () => {
                return new Promise((resolve) => {
                    let attempts = 0;
                    const maxAttempts = 20;
                    
                    const findAndClick = () => {
                        const links = document.querySelectorAll('a');
                        for (const link of links) {
                            const text = link.textContent.trim().toLowerCase();
                            const href = link.getAttribute('href') || '';
                            
                            if (text.includes('đăng nhập bằng email') ||
                                text.includes('tên người dùng') ||
                                href.includes('/login/phone-or-email/email')) {
                                if (link.offsetParent !== null) {
                                    link.click();
                                    resolve(true);
                                    return;
                                }
                            }
                        }
                        
                        attempts++;
                        if (attempts < maxAttempts) {
                            setTimeout(findAndClick, 500);
                        } else {
                            resolve(false);
                        }
                    };
                    
                    findAndClick();
                });
            }
            """
            
            result = self.page.evaluate(js_code)
            
            if result:
                logger.success("Đã chọn đăng nhập bằng email")
                return True
            else:
                logger.error("Không tìm thấy option email")
                return False
                
        except Exception as e:
            logger.error(f"Lỗi khi chọn email option: {e}")
            return False
    
    def fill_credentials(self, credentials: LoginCredentials, timeout: int = 10000) -> bool:
        """Bước 4: Nhập username và password"""
        import random
        
        try:
            logger.info("⌨Đang nhập thông tin đăng nhập...")
            
            # Selectors
            username_selectors = [
                'input[name="username"]',
                'input[name="email"]',
                'input[placeholder*="Email"]',
                'input[placeholder*="username"]',
                'input[type="text"]'
            ]
            
            password_selectors = [
                'input[type="password"]',
                'input[name="password"]'
            ]
            
            def find_visible_input(selectors: List[str]):
                """Tìm input visible"""
                for selector in selectors:
                    loc = self.page.locator(selector)
                    if loc.count() and loc.first.is_visible():
                        return loc.first
                return None
            
            # Nhập username
            PageHelper.safe_sleep(random.uniform(0.5, 1.0))
            username_input = find_visible_input(username_selectors)
            
            if not username_input:
                logger.error("Không tìm thấy ô nhập username")
                return False
            
            username_input.click(timeout=timeout)
            username_input.fill("")  # Xóa nội dung cũ
            
            # Typing như người thật
            for char in credentials.username:
                self.page.keyboard.type(char, delay=random.randint(40, 120))
            
            # Nhập password
            PageHelper.safe_sleep(random.uniform(0.2, 0.6))
            password_input = find_visible_input(password_selectors)
            
            if not password_input:
                self.page.keyboard.press("Tab")
                PageHelper.safe_sleep(0.3)
                password_input = find_visible_input(password_selectors)
            
            if not password_input:
                logger.error("Không tìm thấy ô nhập password")
                return False
            
            password_input.click(timeout=timeout)
            password_input.fill("")
            
            for char in credentials.password:
                self.page.keyboard.type(char, delay=random.randint(50, 140))
            
            logger.success("Đã nhập thông tin đăng nhập")
            return True
            
        except Exception as e:
            logger.error(f"Lỗi khi nhập thông tin: {e}")
            return False
    
    def click_submit_button(self) -> bool:
        """Bước 5: Click nút 'Đăng nhập' để submit"""
        try:
            logger.info("Đang tìm nút 'Đăng nhập' để submit...")
            
            js_code = """
            () => {
                return new Promise((resolve) => {
                    let attempts = 0;
                    const maxAttempts = 20;
                    
                    const findAndClick = () => {
                        const btn = document.querySelector('button[data-e2e="login-button"]');
                        if (btn && btn.offsetParent !== null) {
                            btn.removeAttribute('disabled');
                            btn.click();
                            resolve(true);
                            return;
                        }
                        
                        attempts++;
                        if (attempts < maxAttempts) {
                            setTimeout(findAndClick, 500);
                        } else {
                            resolve(false);
                        }
                    };
                    
                    findAndClick();
                });
            }
            """
            
            result = self.page.evaluate(js_code)
            
            if result:
                logger.success("Đã click nút 'Đăng nhập'")
                return True
            else:
                logger.error("Không tìm thấy nút submit")
                return False
                
        except Exception as e:
            logger.error(f"Lỗi khi click submit: {e}")
            return False
    
    def execute_full_login(self, credentials: LoginCredentials) -> bool:
        """Thực hiện toàn bộ quy trình đăng nhập"""
        logger.info("Bắt đầu quy trình đăng nhập...")
        
        # Bước 1: Click nút Login chính
        if not self.click_main_login_button():
            return False
        
        # Bước 2: Chọn phone/email option
        if not self.select_phone_email_option():
            return False
        
        # Bước 3: Chọn email login
        if not self.select_email_login_option():
            return False
        
        # Bước 4: Nhập thông tin
        if not self.fill_credentials(credentials):
            return False
        
        # Bước 5: Click submit
        if not self.click_submit_button():
            return False
        
        # Đợi hoàn tất
        logger.info("Đang chờ hoàn tất đăng nhập...")
        self.page.wait_for_timeout(15000)
        
        if CaptchaDetector.detect(self.page):
            # logger.warning("Gặp captcha, đóng browser và thử lại...")
            logger.warning("Gặp captcha, Thử giải...")
            pos1, pos2 = get_similar_objects_positions(page=self.page)
            if pos1 == None and pos2 == None:
                return False
                # self.page.wait_for_timeout(15000)
            print(pos1,pos2)

            
        logger.success("Hoàn tất quy trình đăng nhập")
        return True


class HeaderCollector:
    """Class thu thập headers từ request"""
    
    def __init__(self, context: BrowserContext, target_url: str):
        self.context = context
        self.target_url = target_url
        self.collected_headers: Dict = {}
    
    def setup_listener(self):
        """Thiết lập listener để capture headers"""
        def capture_request(request):
            try:
                if self.target_url in request.url:
                    headers = request.headers.copy()
                    
                    # Thêm cookies từ session hiện tại
                    cookies = self.context.cookies()
                    cookie_str = "; ".join([f"{c['name']}={c['value']}" for c in cookies])
                    headers["cookie"] = cookie_str
                    
                    self.collected_headers.update(headers)
                    logger.info(f"📦 Đã capture headers từ {request.url[:50]}...")
            except Exception as e:
                logger.warning(f"⚠️ Lỗi capture request: {e}")
        
        self.context.on("request", capture_request)
    
    def save_to_file(self, filename: str = "./configs/cookies/tiktok_headers_0.json") -> bool:
        """Lưu headers vào file JSON"""
        try:
            if not self.collected_headers:
                logger.error("Không có headers để lưu")
                return False
            
            with open(filename, "w", encoding="utf-8") as f:
                json.dump(self.collected_headers, f, indent=2, ensure_ascii=False)
            
            logger.success(f"Đã lưu headers vào {filename}")
            return True
            
        except Exception as e:
            logger.error(f"Lỗi khi lưu headers: {e}")
            return False


class TikTokAutomation:
    """Class chính điều khiển toàn bộ automation"""
    
    def __init__(
        self,
        target_url: str,
        credentials: LoginCredentials,
        proxy_config: Optional[ProxyConfig] = None,
        headless: bool = False
    ):
        self.target_url = target_url
        self.credentials = credentials
        self.proxy_config = proxy_config
        self.headless = headless
    
    def run(self, max_attempts: int = 10):
        """Chạy automation với retry logic"""
        attempt = 0
        
        with sync_playwright() as p:
            while attempt < max_attempts:
                attempt += 1
                logger.info(f"\n{'='*60}\n🔄 Lần thử #{attempt}/{max_attempts}\n{'='*60}")
                
                if self._run_single_attempt(p):
                    logger.success(" Hoàn tất thành công!")
                    break
                else:
                    logger.warning(f" Lần thử #{attempt} thất bại, thử lại...")
                    PageHelper.safe_sleep(2)
                    if attempt % 2 == 0:
                        logger.info("Reset proxy trước khi thử lại...")
                        PageHelper.safe_sleep(5)
                        self.proxy_config.reset_proxy()
            else:
                logger.error(f" Đã thử {max_attempts} lần nhưng vẫn thất bại")
    
    def _run_single_attempt(self, playwright) -> bool:
        """Thực hiện một lần thử"""
        browser = None
        
        try:
            # Launch browser với proxy (nếu có)
            browser_args = [
                "--disable-blink-features=AutomationControlled",
                "--disable-dev-shm-usage",
                "--no-sandbox",
                "--disable-infobars",
                "--disable-extensions"
            ]
            
            launch_options = {
                "headless": self.headless,
                "args": browser_args
            }
            
            # Thêm proxy nếu có
            if self.proxy_config:
                launch_options["proxy"] = self.proxy_config.to_dict()
                logger.info(f"Sử dụng proxy: {self.proxy_config.server}")
            
            browser = playwright.chromium.launch(**launch_options)

            context = browser.new_context()
            page = context.new_page()
            context.clear_cookies()
            
            # Bước 1: Mở trang và kiểm tra captcha
            logger.info(f"Đang truy cập: {self.target_url}")
            page.goto("https://www.tiktok.com/", wait_until="domcontentloaded", timeout=30000)
            PageHelper.wait_for_ready(page)
            PageHelper.safe_sleep(20)
            
            # Kiểm tra captcha liên tục
            if CaptchaDetector.detect(page):
                logger.warning("Gặp captcha, đóng browser và thử lại...")
                return False
            
            logger.success("Không có captcha!")
            
            # Bước 2: Thực hiện đăng nhập
            login_flow = TikTokLoginFlow(page)
            if not login_flow.execute_full_login(self.credentials):
                logger.error("Đăng nhập thất bại")
                return False
            
            # Bước 3: Mở video và collect headers
            logger.info(f"Mở video: {self.target_url}")
            
            header_collector = HeaderCollector(context, self.target_url)
            header_collector.setup_listener()
            
            page.goto(self.target_url, wait_until="networkidle", timeout=40000)
            page.wait_for_timeout(5000)
            
            # Bước 4: Lưu headers
            if not header_collector.save_to_file():
                raise RuntimeError("Không lấy được headers từ video URL")
            
            # Giữ browser mở để kiểm tra
            logger.info("Giữ browser mở 60 giây để kiểm tra...")
            page.wait_for_timeout(10000)
            
            
            return True
            
        except Exception as e:
            logger.error(f"Lỗi: {e}")
            return False
            
        finally:
            if browser and browser.is_connected():
                browser.close()

def load_proxy_config():
    """Đọc toàn bộ cấu hình YAML"""
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)
    return config


def main():
    """Hàm main"""
    config = load_proxy_config()
    # Cấu hình loguru
    logger.add(
        "tiktok_automation.log",
        rotation="1 MB",
        retention="7 days",
        level="INFO",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}"
    )
    
    # Cấu hình
    TARGET_URL = "https://www.tiktok.com/@missnatalie9791/video/7565661044358663454"
    
    credentials = LoginCredentials(
        username=config["account"]["username"],
        password=config["account"]["password"]
    )
    
    
    # Cấu hình proxy (có thể None để không dùng proxy)
    proxy_cfg = config["proxy_newen"]
    proxy_config = ProxyConfig(
        server=proxy_cfg["server"],
        username=proxy_cfg["username"],
        password=proxy_cfg["password"],
        link_request_proxy=proxy_cfg["link_request_proxy"]
    )
    
    # Hoặc không dùng proxy:
    # proxy_config = None
    
    # Khởi chạy automation
    automation = TikTokAutomation(
        target_url=TARGET_URL,
        credentials=credentials,
        proxy_config=proxy_config,  # Có thể set None để tắt proxy
        headless=False
    )
    
    automation.run(max_attempts=10)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Bye!")