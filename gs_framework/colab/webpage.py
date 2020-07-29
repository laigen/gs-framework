import time
from typing import NamedTuple, Any, Iterable, Union

from selenium import webdriver
from selenium.common.exceptions import NoAlertPresentException, StaleElementReferenceException
from selenium.webdriver.chrome.webdriver import WebDriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions


class SeleniumLocator(NamedTuple):
    by: str
    value: str


def _expand_shadow_element(driver: WebDriver, element):
    shadow_root = driver.execute_script('return arguments[0].shadowRoot', element)
    return shadow_root


def to_locators(locator: Union[SeleniumLocator, Iterable[SeleniumLocator]]) -> Iterable[SeleniumLocator]:
    return [locator] if isinstance(locator, Iterable) and len(locator) > 0 \
                        and isinstance(next(locator.__iter__()), str) else locator


def find_element(driver: WebDriver, locator: Union[SeleniumLocator, Iterable[SeleniumLocator]]):
    locators_of_each_dom_level = to_locators(locator)
    el = None
    for (by, value) in locators_of_each_dom_level:
        root_el = driver if el is None else _expand_shadow_element(driver, el)
        el = root_el.find_element(by, value)

    return el


expected_conditions._find_element = find_element


class DOM:

    @staticmethod
    def js_4_css_selector(css_selector: str, root_el: str = 'document'):
        return f"{root_el}.querySelector('{css_selector}')"

    @staticmethod
    def js_4_el_id(el_id: str, root_el: str = 'document'):
        return f"{root_el}.getElementById('{el_id}')"

    @staticmethod
    def js_4_func_call(js_4_obj: str, func_name: str, *paras: Any):
        # we don't do string escape here until it's necessary to do so
        paras_str = ','.join(map(lambda p: f"'{p}'" if isinstance(p, str) else p, paras))
        return f"{js_4_obj}.{func_name}({paras_str})"

    @staticmethod
    def js_from_locator(locator: Union[SeleniumLocator, Iterable[SeleniumLocator]]):
        locators_of_each_dom_level = to_locators(locator)
        root_el = 'document'
        for (by, value) in locators_of_each_dom_level:
            if By.ID == by:
                js = DOM.js_4_el_id(value, root_el)
            elif By.CSS_SELECTOR == by:
                js = DOM.js_4_css_selector(value, root_el)
            else:
                raise RuntimeError(f"Unsupported locator by: {by}")

            root_el = f"{js}.shadowRoot"

        return js

    @staticmethod
    def js_click(driver: WebDriver, locator: Union[SeleniumLocator, Iterable[SeleniumLocator]]):
        driver.execute_script(DOM.js_4_func_call(DOM.js_from_locator(locator), "click"))

    @staticmethod
    def js_4_dispatch_event(js_4_obj: str, event_type: str, event_name: str):
        return f"var sim_event = document.createEvent('{event_type}'); " \
               f"sim_event.initEvent('{event_name}', true, true); " \
               f"{js_4_obj}.dispatchEvent(sim_event);"

    @staticmethod
    def js_4_mouse_down(js_4_obj: str):
        return DOM.js_4_dispatch_event(js_4_obj, 'MouseEvents', 'mousedown')

    @staticmethod
    def js_4_mouse_up(js_4_obj: str):
        return DOM.js_4_dispatch_event(js_4_obj, 'MouseEvents', 'mouseup')


class ChromeDriver:

    # the path of chrome driver inside container creating chrome driver instance
    path_4_chrome_driver = "/usr/local/bin/chromedriver"

    @staticmethod
    def attach_to(address: str, chrome_debug_port: int) -> WebDriver:
        from selenium.webdriver.chrome.options import Options
        chrome_options = Options()
        chrome_options.add_experimental_option("debuggerAddress", f"{address}:{chrome_debug_port}")
        driver = webdriver.Chrome(ChromeDriver.path_4_chrome_driver, chrome_options=chrome_options)
        return driver


class WebPage:

    def __init__(self, url: str, run_at: str, port: int = 9222):
        super().__init__()

        self._run_at = run_at
        self._port = port
        self.driver: WebDriver = None

        self._recreate_driver()

        if url:
            current_url = self.driver.current_url
            # if url != current_url and not current_url.startswith(f"{url}#"):
            # reload the page even if url is the same

            # it might throw selenium.common.exceptions.WebDriverException for disconnection. let the executor retry
            self.driver.get(url)

            time.sleep(5)  # let possible site leaving window appear

            try:
                self.driver.switch_to.alert.accept()
            except NoAlertPresentException:
                pass

    def _recreate_driver(self):
        import re
        import dns.resolver

        pattern_4_ip = re.compile(r"\d+\.\d+\.\d+.\d+")
        run_at_ip = str(dns.resolver.query(self._run_at)[0]) if pattern_4_ip.fullmatch(self._run_at) is None \
            else self._run_at
        self.driver = ChromeDriver.attach_to(run_at_ip, self._port)

    def wait(self, seconds: int = None):
        return WebDriverWait(self.driver, seconds or 120, ignored_exceptions=(StaleElementReferenceException, ))

    def refresh(self, seconds: int = None):
        self.driver.refresh()
        if seconds:
            time.sleep(seconds)

    def js_click(self, locator: Union[SeleniumLocator, Iterable[SeleniumLocator]]):
        DOM.js_click(self.driver, locator)

    @property
    def title(self):
        return self.driver.title


# Manually login to google account and save login status in user data folder so this class is not used.
# class GoogleLogin(WebPage):
#
#     def __init__(self, driver: WebDriver, email: str, passwd: str):
#         super().__init__(driver, 'https://www.google.com/intl/en_us/')
#
#         # click the sign in button
#         self.wait().until(EC.element_to_be_clickable((By.ID, "gb_70"))).click()
#
#         # note that if sign out and sign in again, there will be a page to let user select account
#
#         # input account email
#         self.wait().until(EC.element_to_be_clickable((By.ID, "identifierId"))).send_keys(email)
#
#         # click next
#         self.wait().until(EC.element_to_be_clickable((By.ID, "identifierNext"))).click()
#
#         # input password
#         self.wait().until(EC.element_to_be_clickable((By.NAME, "password"))).send_keys(passwd)
#
#         # click next
#         self.wait().until(EC.element_to_be_clickable((By.ID, "passwordNext"))).click()
#
#         # wait for the page after login is rendered
#         el_account_icon = self.wait().until(EC.element_to_be_clickable((By.CSS_SELECTOR, "#gbw a.gb_D.gb_Ha.gb_i")))
#         self.wait().until(lambda d: "Google Account" in el_account_icon.get_property('title'))
