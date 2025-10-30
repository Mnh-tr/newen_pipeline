from playwright.sync_api import sync_playwright
from loguru import logger
import time
import random
import json
import requests
def check_captcha(page):
    """Kiểm tra xem có captcha xuất hiện hay không"""
    try:
        captcha_selectors = [
            'iframe[id*="captcha"]',
            '[class*="captcha"]',
            '#captcha-verify-image',
            '.captcha_verify_container',
            'div[id*="captcha"]'
        ]
        
        for selector in captcha_selectors:
            if page.locator(selector).count() > 0:
                logger.warning("Phát hiện captcha!")
                return True
        
        if page.locator('text=/verify|captcha/i').count() > 0:
            logger.warning("Phát hiện captcha!")
            return True
            
        return False
    except Exception as e:
        logger.error(f"Lỗi khi kiểm tra captcha: {e}")
        return False

def wait_for_page_ready(page):
    """Đợi trang load hoàn toàn"""
    try:
        page.wait_for_load_state('networkidle', timeout=30000)
        logger.success("Trang đã load hoàn toàn")
        return True
    except Exception as e:
        logger.warning(f"Timeout khi đợi load: {e}")
        return False

def click_login_button(page):
    """Click vào nút Login bằng JavaScript"""
    try:
        logger.info("Đang tìm nút Login...")
        
        # Đợi 2 giây để đảm bảo UI đã render
        time.sleep(2)
        
        js_code = """
        () => {
            return new Promise((resolve) => {
                // Đợi tối đa 10 giây để tìm nút
                let attempts = 0;
                const maxAttempts = 20;
                
                const findAndClick = () => {
                    // Tìm nút login theo ID
                    let loginBtn = document.querySelector('#top-right-action-bar-login-button');
                    
                    // Nếu không tìm thấy theo ID, thử tìm theo class
                    if (!loginBtn) {
                        loginBtn = document.querySelector('.css-1tvowuz-5e6d46e3--StyledPrimaryLoginButton');
                    }
                    
                    // Nếu vẫn không tìm thấy, tìm theo text
                    if (!loginBtn) {
                        let buttons = document.querySelectorAll('button');
                        for (let btn of buttons) {
                            if (btn.textContent.includes('Log in')) {
                                loginBtn = btn;
                                break;
                            }
                        }
                    }
                    
                    // Nếu tìm thấy và nút visible
                    if (loginBtn && loginBtn.offsetParent !== null) {
                        loginBtn.click();
                        resolve(true);
                        return;
                    }
                    
                    // Retry nếu chưa tìm thấy
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
        
        result = page.evaluate(js_code)
        
        if result:
            logger.success("Đã click vào nút Login")
            return True
        else:
            logger.error("Không tìm thấy nút Login")
            return False
            
    except Exception as e:
        logger.error(f"Lỗi khi click login: {e}")
        return False

def click_phone_email_option(page):
    """Click vào option 'Use phone / email / username' bằng JavaScript"""
    try:
        logger.info("Đang tìm option 'Use phone / email / username'...")
        
        # Đợi modal login xuất hiện
        time.sleep(3)
        
        js_code = """
        () => {
            return new Promise((resolve) => {
                let attempts = 0;
                const maxAttempts = 20;
                
                const findAndClick = () => {
                    // Tìm theo data-e2e attribute
                    let options = document.querySelectorAll('[data-e2e="channel-item"]');
                    
                    // Tìm option chứa text phù hợp
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
                    
                    // Nếu không tìm thấy, thử tìm theo role link
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
                    
                    // Retry nếu chưa tìm thấy
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
        
        result = page.evaluate(js_code)
        
        if result:
            logger.success("Đã click vào 'Use phone / email / username'")
            return True
        else:
            logger.error("Không tìm thấy option")
            return False
            
    except Exception as e:
        logger.error(f"Lỗi khi click option: {e}")
        return False

def click_email_login_option(page):
    """Click vào nút 'Đăng nhập bằng email hoặc tên người dùng' bằng JavaScript"""
    try:
        logger.info("Đang tìm nút 'Đăng nhập bằng email hoặc tên người dùng'...")

        time.sleep(2)  # đợi modal hiển thị

        js_code = """
        () => {
            return new Promise((resolve) => {
                let attempts = 0;
                const maxAttempts = 20;

                const findAndClick = () => {
                    // Lấy tất cả thẻ <a>
                    const links = document.querySelectorAll('a');
                    for (const link of links) {
                        const text = link.textContent.trim().toLowerCase();
                        const href = link.getAttribute('href') || '';
                        // Kiểm tra theo text hoặc href
                        if (
                            text.includes('đăng nhập bằng email') ||
                            text.includes('tên người dùng') ||
                            href.includes('/login/phone-or-email/email')
                        ) {
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

        result = page.evaluate(js_code)

        if result:
            logger.success("Đã click vào 'Đăng nhập bằng email hoặc tên người dùng'")
            return True
        else:
            logger.error("Không tìm thấy nút 'Đăng nhập bằng email hoặc tên người dùng'")
            return False

    except Exception as e:
        logger.error(f"Lỗi khi click 'Đăng nhập bằng email hoặc tên người dùng': {e}")
        return False

def fill_tiktok_login(page, username: str, password: str, timeout: int = 10000):
    """Nhập username + password bằng Playwright actions (ngắn gọn)."""
    import random, time
    try:
        logger.info("Nhập username/password (actions)...")

        # selectors phổ biến
        user_sel = [
            'input[name="username"]','input[name="email"]',
            'input[placeholder*="Email"]','input[placeholder*="username"]',
            'input[type="text"]'
        ]
        pass_sel = ['input[type="password"]','input[name="password"]']

        def find_visible(selectors):
            for s in selectors:
                loc = page.locator(s)
                if loc.count() and loc.first.is_visible():
                    return loc.first
            return None

        time.sleep(random.uniform(0.5,1.0))
        u = find_visible(user_sel)
        if not u:
            logger.error("Không tìm username input"); return False
        u.click(timeout=timeout)
        u.fill("")  # xóa
        for ch in username:
            page.keyboard.type(ch, delay=random.randint(40,120))

        time.sleep(random.uniform(0.2,0.6))
        p = find_visible(pass_sel)
        if not p:
            page.keyboard.press("Tab"); time.sleep(0.3)
            p = find_visible(pass_sel)
        if not p:
            logger.error("Không tìm password input"); return False
        p.click(timeout=timeout)
        p.fill("")
        for ch in password:
            page.keyboard.type(ch, delay=random.randint(50,140))

        logger.success("Đã nhập username & password")
        return True
    except Exception as e:
        logger.error(f"Lỗi nhập: {e}")
        return False

def click_login_button_2(page):
    """Click vào nút 'Đăng nhập' bằng JavaScript"""
    try:
        logger.info("Đang tìm nút 'Đăng nhập'...")

        js_code = """
        () => {
            return new Promise((resolve) => {
                let attempts = 0;
                const maxAttempts = 20;

                const findAndClick = () => {
                    const btn = document.querySelector('button[data-e2e="login-button"]');
                    if (btn && btn.offsetParent !== null) {
                        // Nếu nút đang disabled thì bỏ thuộc tính disabled
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

        result = page.evaluate(js_code)
        if result:
            logger.success("Đã click vào nút 'Đăng nhập'")
            return True
        else:
            logger.error("Không tìm thấy nút 'Đăng nhập'")
            return False

    except Exception as e:
        logger.error(f"Lỗi khi click nút 'Đăng nhập': {e}")
        return False


def open_tiktok_with_retry():
    """Login TikTok, sau đó mở video và lưu header"""
    target_url = "https://www.tiktok.com/@wondermama_94/video/7562021847189998879"
    attempt = 0

    with sync_playwright() as p:
        while True:
            attempt += 1
            logger.info(f"\n{'='*50}\nLần thử #{attempt}\n{'='*50}")
            # 103.207.36.217:8039:pIVxYmgTstyle:DzAFKtyz
            browser = p.chromium.launch(
                headless=False,
                proxy={
                    "server": "http://103.207.36.217:8039",        # hoặc "socks5://ip:port"
                    "username": "pIVxYmgTstyle",                # nếu có
                    "password": "DzAFKtyz",                # nếu có
                },
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--disable-dev-shm-usage",
                    "--no-sandbox",
                    "--disable-infobars",
                    "--disable-extensions"
                ],
            )
            context = browser.new_context()
            page = context.new_page()
            context.clear_cookies()

            try:
                # ===== MỞ TRANG CHÍNH =====
                logger.info("Đang truy cập TikTok...")
                page.goto("https://www.tiktok.com/@wondermama_94/video/7562021847189998879", wait_until="domcontentloaded", timeout=30000)
                wait_for_page_ready(page)
                time.sleep(20)
                if check_captcha(page):
                    logger.warning("Gặp captcha, đóng browser và thử lại...")
                    browser.close()
                    time.sleep(2)
                    continue

                logger.success("Không có captcha!")

                # ===== LOGIN =====
                if click_login_button(page):
                    click_phone_email_option(page)
                    click_email_login_option(page)
                    fill_tiktok_login(page, username="user4499981277330", password="367K#26N")
                    click_login_button_2(page)
                    logger.info("Đang chờ hoà qn tất đăng nhập...")
                    page.wait_for_timeout(15000)
                else:
                    logger.error("Không click được nút Login.")
                    browser.close()
                    time.sleep(2)
                    continue

                # ===== MỞ VIDEO SAU KHI LOGIN =====
                logger.info(f"Mở video: {target_url}")

                collected_headers = {}

                def capture_request(request):
                    try:
                        if target_url in request.url:
                            headers = request.headers.copy()

                            # thêm cookie thật của session hiện tại
                            cookies = context.cookies()
                            cookie_str = "; ".join([f"{c['name']}={c['value']}" for c in cookies])
                            headers["cookie"] = cookie_str

                            collected_headers.update(headers)
                    except Exception as e:
                        logger.warning(f"Lỗi capture request: {e}")

                context.on("request", capture_request)
                page.goto(target_url, wait_until="networkidle", timeout=40000)
                page.wait_for_timeout(5000)

                # ===== LƯU HEADER =====
                if collected_headers:
                    with open("headers.json", "w", encoding="utf-8") as f:
                        json.dump(collected_headers, f, indent=2, ensure_ascii=False)
                    logger.success("Đã lưu header vào headers.json")
                else:
                    raise RuntimeError(" Không lấy được header từ video URL")

                logger.info("Hoàn tất, giữ browser mở 1 phút để kiểm tra.")
                page.wait_for_timeout(60000)
                break

            except Exception as e:
                logger.error(f"Lỗi: {e}")
                raise e
            finally:
                if browser.is_connected():
                    browser.close()

if __name__ == "__main__":
    # Cấu hình loguru
    logger.add("tiktok_automation.log", rotation="1 MB", retention="7 days", level="INFO")

    
    try:
        open_tiktok_with_retry()
    except KeyboardInterrupt:
        logger.info("Tạm biệt!")