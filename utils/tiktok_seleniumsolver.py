from __future__ import annotations

import base64
import math
import time

import requests
from playwright.sync_api import FloatRect
from selenium.webdriver.common.actions.action_builder import ActionBuilder
from selenium.webdriver.common.actions.interaction import POINTER_MOUSE
from selenium.webdriver.common.actions.pointer_input import PointerInput
from selenium.webdriver.common.by import By
from selenium.webdriver.remote.webelement import WebElement
from tenon import notch_identify, rotate_identify
from undetected_chromedriver import logging
from utils import driver_helper

PUZZLE_UNIQUE_IDENTIFIER = '//div[@class="cap-flex cap-flex-col cap-relative"]'
ROTATE_UNIQUE_IDENTIFIER = '//div[@class="cap-flex cap-flex-col cap-justify-center cap-items-center "]'


class CaptchaType:
    PUZZLE = 1
    ROTATE = 2


def get_box_center(box: FloatRect) -> tuple[float, float]:
    """Get the center of a box from a FloatRect"""
    center_x = box["x"] + (box["width"] / 2)
    center_y = box["y"] + (box["height"] / 2)
    return center_x, center_y


def _get_element_bounding_box(e: WebElement) -> FloatRect:
    loc = e.location
    size = e.size
    return {"x": loc["x"], "y": loc["y"], "width": size["width"], "height": size["height"]}


def get_as_base64(url: str, to_str: bool = False):
    if to_str:
        return base64.b64encode(requests.get(url).content).decode("utf-8")
    return base64.b64encode(requests.get(url).content)

def get_base64_from_web(driver, request_list, url) -> str | None:
    for request in request_list:
        if url in request["url"]:
            request_id = request["request_id"]
            response = driver.execute_cdp_cmd("Network.getResponseBody", {"requestId": request_id})  
            return response["body"]
    return None


def _any_selector_in_list_present(driver, selectors: list[str]) -> bool:
    for selector in selectors:
        for ele in driver.find_elements(By.XPATH, selector):
            if ele.is_displayed():
                logging.debug("Detected selector: " + selector + " from list " + ", ".join(selectors))
                return True
    logging.debug("No selector in list found: " + ", ".join(selectors))
    return False


def identify_captcha(driver):
    for _ in range(10):
        if _any_selector_in_list_present(driver, [PUZZLE_UNIQUE_IDENTIFIER]):
            logging.debug("detected puzzle")
            return CaptchaType.PUZZLE
        if _any_selector_in_list_present(driver, [ROTATE_UNIQUE_IDENTIFIER]):
            logging.debug("detected rorate")
            return CaptchaType.ROTATE
        time.sleep(0.5)
    # raise ValueError("Neither puzzle, shapes, or rotate captcha was present.")
    return None


def solve_puzzle(driver):
    div_imgs = driver.find_element(By.XPATH, PUZZLE_UNIQUE_IDENTIFIER)
    imgs = div_imgs.find_elements(By.XPATH, "//img")
    piece = get_as_base64(imgs[-1].get_attribute("src"), True)
    background = get_as_base64(imgs[-2].get_attribute("src"), True)
    result = notch_identify(piece, background, image_type=0)

    draggable = driver.find_element(By.XPATH, '//div[@class="cap-flex cap-absolute "]')
    slide_button_box = _get_element_bounding_box(draggable)
    start_x, start_y = get_box_center(slide_button_box)
    input = PointerInput(POINTER_MOUSE, "default mouse")
    actions = ActionBuilder(driver, duration=5, mouse=input)
    _ = actions.pointer_action.move_to_location(start_x, start_y).pointer_down()
    background_width = 552
    slide_box_width = 287
    start_distance = int(result / (background_width / slide_box_width))
    for pixel in range(start_distance):
        _ = actions.pointer_action.move_to_location(int(start_x + pixel), int(start_y + math.log(1 + pixel))).pause(
            0.02
        )
    actions.pointer_action.pause(0.5)
    _ = actions.pointer_action.pointer_up()
    actions.perform()

    return driver

def solve_rotate_v2(driver):
    """Solve rotate captcha tiktok v2
    Images now be stored in blob:https://

    Unavailable to get base64 via requests:
    -> error: InvalidSchema: No connection adapters were found for 'blob:https://www.tiktok.com/faed9be2-f55e-400c-84f4-166f8505e52d'

    Solution: get from traffic logs | selenium


    Args:
        driver (WebDriver): WebDriver client

    Returns:
        WebDriver: WebDriver client
    """
    div_imgs = driver.find_element(By.XPATH, ROTATE_UNIQUE_IDENTIFIER)
    imgs = div_imgs.find_elements(By.XPATH, "//img")
    request_list = driver_helper.get_traffic_network_from_driver(driver)
    inner = get_base64_from_web(driver, request_list, imgs[-1].get_attribute("src"))
    outer = get_base64_from_web(driver, request_list, imgs[-2].get_attribute("src"))
    if not inner or not outer:
        print("Cannot found inner or outer")
        return driver
    result = rotate_identify(inner, outer)

    draggable = driver.find_element(By.XPATH, '//div[@class="cap-flex cap-absolute "]')
    slide_button_box = _get_element_bounding_box(draggable)
    start_x, start_y = get_box_center(slide_button_box)
    input = PointerInput(POINTER_MOUSE, "default mouse")
    actions = ActionBuilder(driver, duration=5, mouse=input)
    _ = actions.pointer_action.move_to_location(start_x, start_y).pointer_down()
    max_rotate_degree = 284
    slide_box_width = 180
    start_distance = int(result.inner_rotate_angle * max_rotate_degree / slide_box_width)
    for pixel in range(start_distance):
        _ = actions.pointer_action.move_to_location(int(start_x + pixel), int(start_y + math.log(1 + pixel))).pause(
            0.02
        )
    actions.pointer_action.pause(0.5)
    _ = actions.pointer_action.pointer_up()
    actions.perform()
    return driver


def solve_puzzle_v2(driver):
    """Solve puzzle captcha tiktok v2
    Images now be stored in blob:https://

    Unavailable to get base64 via requests:
    -> error: InvalidSchema: No connection adapters were found for 'blob:https://www.tiktok.com/faed9be2-f55e-400c-84f4-166f8505e52d'

    Solution: get from traffic logs | selenium


    Args:
        driver (WebDriver): WebDriver client

    Returns:
        WebDriver: WebDriver client
    """
    div_imgs = driver.find_element(By.XPATH, PUZZLE_UNIQUE_IDENTIFIER)
    imgs = div_imgs.find_elements(By.XPATH, "//img")
    if len(imgs) < 2:
        logging.error("Error: Not enough images to solve captcha.")
        return driver

    request_list = driver_helper.get_traffic_network_from_driver(driver)
    
    piece = get_base64_from_web(driver, request_list, imgs[-1].get_attribute("src"))
    background = get_base64_from_web(driver, request_list, imgs[-2].get_attribute("src"))
    if not piece or not background:
        print("Cannot found piece or background")
        return driver
    result = notch_identify(piece, background, image_type=0)

    draggable = driver.find_element(By.XPATH, '//div[@class="cap-flex cap-absolute "]')
    slide_button_box = _get_element_bounding_box(draggable)
    start_x, start_y = get_box_center(slide_button_box)
    input = PointerInput(POINTER_MOUSE, "default mouse")
    actions = ActionBuilder(driver, duration=5, mouse=input)
    _ = actions.pointer_action.move_to_location(start_x, start_y).pointer_down()
    background_width = 552
    slide_box_width = 287
    start_distance = int(result / (background_width / slide_box_width))
    for pixel in range(start_distance):
        _ = actions.pointer_action.move_to_location(int(start_x + pixel), int(start_y + math.log(1 + pixel))).pause(
            0.02
        )
    actions.pointer_action.pause(0.5)
    _ = actions.pointer_action.pointer_up()
    actions.perform()

    return driver

def get_blob_as_base64(driver, blob_url):
    script = """
    var url = arguments[0];
    var callback = arguments[arguments.length - 1];
    fetch(url)
        .then(response => response.blob())
        .then(blob => {
            var reader = new FileReader();
            reader.onloadend = function() { callback(reader.result); };
            reader.readAsDataURL(blob);
        })
        .catch(error => callback('ERROR: ' + error.message));
    """
    result = driver.execute_async_script(script, blob_url)
    if result.startswith('data:'):
        return result.split(',')[1]
    else:
        raise Exception(f"Failed to fetch blob URL: {result}")


def solve_rotate(driver):
    div_imgs = driver.find_element(By.XPATH, ROTATE_UNIQUE_IDENTIFIER)
    imgs = div_imgs.find_elements(By.XPATH, ".//img")

    inner_src = imgs[-1].get_attribute("src")
    outer_src = imgs[-2].get_attribute("src")

    inner = get_blob_as_base64(driver, inner_src)
    outer = get_blob_as_base64(driver, outer_src)
    result = rotate_identify(inner, outer)
    

    # inner = get_as_base64(imgs[-1].get_attribute("src"), True)
    # outer = get_as_base64(imgs[-2].get_attribute("src"), True)
    # result = rotate_identify(inner, outer)

    draggable = driver.find_element(By.XPATH, '//div[@class="cap-flex cap-absolute "]')
    slide_button_box = _get_element_bounding_box(draggable)
    start_x, start_y = get_box_center(slide_button_box)
    input = PointerInput(POINTER_MOUSE, "default mouse")
    actions = ActionBuilder(driver, duration=5, mouse=input)
    _ = actions.pointer_action.move_to_location(start_x, start_y).pointer_down()
    max_rotate_degree = 284
    slide_box_width = 180
    start_distance = int(result.inner_rotate_angle * max_rotate_degree / slide_box_width)
    for pixel in range(start_distance):
        _ = actions.pointer_action.move_to_location(int(start_x + pixel), int(start_y + math.log(1 + pixel))).pause(
            0.02
        )
    actions.pointer_action.pause(0.5)
    _ = actions.pointer_action.pointer_up()
    actions.perform()
    return driver
