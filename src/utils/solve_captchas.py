import cv2
import numpy as np
import os
import glob
import time
import base64
def download_blob_image(page, selector, filename="image.png", retries=3):
    """
    Tải ảnh từ thẻ <img> có src dạng blob.
    Tự động chờ ảnh xuất hiện và retry nếu DOM bị reload.
    """
    for attempt in range(retries):
        try:
            # Đợi phần tử xuất hiện và DOM ổn định
            # page.wait_for_load_state("networkidle", timeout=10000)
            # page.wait_for_selector(selector, timeout=10000)

            # Lấy dữ liệu ảnh từ DOM (base64)
            img_data = page.eval_on_selector(selector, """
                (img) => {
                    const canvas = document.createElement('canvas');
                    canvas.width = img.naturalWidth;
                    canvas.height = img.naturalHeight;
                    const ctx = canvas.getContext('2d');
                    ctx.drawImage(img, 0, 0);
                    return canvas.toDataURL('image/png').split(',')[1];
                }
            """)

            # Lưu ảnh
            img_bytes = base64.b64decode(img_data)
            with open(filename, "wb") as f:
                f.write(img_bytes)
            print(f"✅ Đã lưu ảnh: {filename}")
            return

        except TimeoutError:
            print(f"⚠️ Ảnh chưa load, thử lại lần {attempt + 1}/{retries}...")
            time.sleep(2)
        except Exception as e:
            print(f"⚠️ Lỗi khi lấy ảnh (lần {attempt + 1}/{retries}): {e}")
            time.sleep(2)

    print("Không thể tải ảnh sau nhiều lần thử.")

def detect_and_cut_objects():
    # 1️⃣ Đọc ảnh
    image_path = r"D:\nhi_workspace\newen_pipeline\configs\captcha\img_captcha.webp"
    img = cv2.imread(image_path)

    h, w = img.shape[:2]

    # 2️⃣ Chuyển sang grayscale
    gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)

    # 3️⃣ Nhị phân hóa (giả sử nền sáng, đối tượng tối)
    _, mask = cv2.threshold(gray, 200, 255, cv2.THRESH_BINARY_INV)

    # 4️⃣ Làm sạch mask bằng morphology
    kernel = cv2.getStructuringElement(cv2.MORPH_ELLIPSE, (3, 3))
    mask = cv2.morphologyEx(mask, cv2.MORPH_OPEN, kernel, iterations=2)

    # 5️⃣ Tách object bằng connected components
    num_labels, labels = cv2.connectedComponents(mask)

    save_dir = r"D:\nhi_workspace\newen_pipeline\configs\captcha"
    os.makedirs(save_dir, exist_ok=True)

    count = 0
    for label in range(1, num_labels):  # 0 là background
        ys, xs = np.where(labels == label)
        if len(xs) == 0 or len(ys) == 0:
            continue
        x1, y1 = xs.min(), ys.min()
        x2, y2 = xs.max(), ys.max()
        obj = img[y1:y2 + 1, x1:x2 + 1]

        out_path = os.path.join(save_dir, f"object_{count}.png")
        cv2.imwrite(out_path, obj)
        count += 1

    print(f"🏁 Hoàn tất! Tổng cộng cắt được {count} đối tượng.")

def find_most_similar_objects():
    # 1️⃣ Đọc danh sách ảnh object
    print("Đọc ảnh object đã cắt...")
    files = glob.glob(r"D:\nhi_workspace\newen_pipeline\configs\captcha\object_*.png")
    if len(files) < 2:
        print("Không đủ ảnh để so sánh.")
        return None  # Không đủ ảnh để so sánh

    print(f"Tìm trong {len(files)} ảnh object...")
    images = [cv2.imread(f) for f in files]

    # 2️⃣ Tìm contour chính cho từng ảnh
    contours_list = []
    for img in images:
        if img is None:
            contours_list.append(None)
            continue
        gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
        _, thresh = cv2.threshold(gray, 127, 255, cv2.THRESH_BINARY)
        contours, _ = cv2.findContours(thresh, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
        main_cnt = max(contours, key=cv2.contourArea) if contours else None
        contours_list.append(main_cnt)

    print("So sánh hình dạng các đối tượng...")
    # 3️⃣ So sánh hình dạng
    min_score = float('inf')
    best_pair = (None, None)

    print("Đang tìm cặp đối tượng giống nhau nhất...")
    for i in range(len(contours_list)):
        for j in range(i + 1, len(contours_list)):
            cnt1, cnt2 = contours_list[i], contours_list[j]
            if cnt1 is None or cnt2 is None:
                continue
            score = cv2.matchShapes(cnt1, cnt2, cv2.CONTOURS_MATCH_I1, 0.0)
            if score < min_score:
                min_score = score
                best_pair = (files[i], files[j])
    
    print("Best pair 1:", best_pair[0])
    print("Best pair 2:", best_pair[1])
    return best_pair

def locate_similar_objects(obj_path1, obj_path2):
    """
    Xác định vị trí của 2 object trong ảnh gốc.
    Đầu vào:
        obj_path1, obj_path2: đường dẫn tới 2 ảnh object giống nhau.
    Đầu ra:
        (top_left1, top_left2): tuple chứa tọa độ (x, y) của 2 object trong ảnh gốc.
    """
    # 1️⃣ Đọc và chuẩn hóa ảnh gốc
    img_orig_resized = cv2.imread(r"D:\nhi_workspace\newen_pipeline\configs\captcha\img_captcha.webp")
    if img_orig_resized is None:
        return None, None
    gray_orig = cv2.cvtColor(img_orig_resized, cv2.COLOR_BGR2GRAY)

    # 2️⃣ Đọc 2 object
    obj1 = cv2.imread(obj_path1)
    obj2 = cv2.imread(obj_path2)
    if obj1 is None or obj2 is None:
        return None, None

    gray_obj1 = cv2.cvtColor(obj1, cv2.COLOR_BGR2GRAY)
    gray_obj2 = cv2.cvtColor(obj2, cv2.COLOR_BGR2GRAY)

    # 3️⃣ Template matching từng object
    res1 = cv2.matchTemplate(gray_orig, gray_obj1, cv2.TM_CCOEFF_NORMED)
    res2 = cv2.matchTemplate(gray_orig, gray_obj2, cv2.TM_CCOEFF_NORMED)

    _, _, _, max_loc1 = cv2.minMaxLoc(res1)
    _, _, _, max_loc2 = cv2.minMaxLoc(res2)

    top_left1 = max_loc1
    top_left2 = max_loc2

    return top_left1, top_left2

def get_similar_objects_positions(page):
    """
    Hàm tổng hợp:
    1. Cắt các object từ ảnh gốc.
    2. Tìm cặp object giống nhau nhất.
    3. Xác định vị trí của 2 object trong ảnh gốc.

    Đầu ra:
        (pos1, pos2): tuple chứa tọa độ (x, y) của 2 object trong ảnh gốc.
    """
    # B1: Tải ảnh captcha từ trang web
    download_blob_image(page=page, selector="img.cap-rounded-lg", filename=r"D:\nhi_workspace\newen_pipeline\configs\captcha\img_captcha.webp")


    # B2: Cắt object từ ảnh
    detect_and_cut_objects()

    print("Tìm cặp object giống nhau nhất...")
    # B3: Tìm cặp object giống nhau nhất
    best_pair = find_most_similar_objects()

    if not best_pair or best_pair[0] is None or best_pair[1] is None:
        return None, None

    # B4: Xác định vị trí 2 object trong ảnh gốc
    pos1, pos2 = locate_similar_objects(best_pair[0], best_pair[1])
    return pos1, pos2

