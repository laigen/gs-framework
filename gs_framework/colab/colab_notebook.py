import re
import time

from enum import Enum
from typing import Iterable, Optional, List, Tuple, Dict, Set

import selenium
from selenium.common.exceptions import TimeoutException
from selenium.webdriver import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.remote.webelement import WebElement
from selenium.webdriver.support import expected_conditions as EC

from gs_framework.colab.cell_option_parser import CellOptions, CellOptionParser
from gs_framework.colab.webpage import WebPage, SeleniumLocator


class CellClass:
    execution_error = 'execution-error'
    focused = 'focused'
    running = 'running'
    pending = 'pending'
    code = 'code'


class CellStatus(Enum):
    running = 1
    pending = 2
    not_running_without_error = 3
    not_running_having_error = 4


def is_cell_status_stopped(cell_status: CellStatus) -> bool:
    return cell_status in (CellStatus.not_running_without_error, CellStatus.not_running_having_error)


def is_code_cell_from_cell_classes(cell_classes: Iterable[str]) -> bool:
    return CellClass.code in cell_classes


def is_cell_focused_from_cell_classes(cell_classes: Iterable[str]) -> bool:
    return CellClass.focused in cell_classes


def cell_status_from_cell_classes(cell_classes: Iterable[str]) -> CellStatus:
    assert is_code_cell_from_cell_classes(cell_classes)
    if CellClass.running in cell_classes:
        return CellStatus.running
    elif CellClass.pending in cell_classes:
        return CellStatus.pending
    elif CellClass.execution_error in cell_classes:
        return CellStatus.not_running_having_error
    else:
        return CellStatus.not_running_without_error


def get_cell_classes(el_cell: WebElement) -> List[str]:
    return el_cell.get_attribute('class').split()


def get_cell_status(el_cell: WebElement) -> CellStatus:
    return cell_status_from_cell_classes(get_cell_classes(el_cell))


def is_code_cell(el_cell: WebElement) -> bool:
    return is_code_cell_from_cell_classes(get_cell_classes(el_cell))


def is_cell_focused(el_cell: WebElement) -> bool:
    return is_cell_focused_from_cell_classes(get_cell_classes(el_cell))

# def button_title_2_cell_status(button_title: str) -> CellStatus:
#     cell_status = CellStatus.running_occupying_vm if button_title.find("Interrupt execution") != -1 \
#         else CellStatus.running_waiting_for_vm if button_title.find("currently executing") != -1 \
#         else CellStatus.not_running
#     # if CellStatus.not_running == cell_status:
#     #     print(f"Button title for not running: {button_title}")
#     return cell_status


class BackendNotAvailableError(Exception):
    pass


class VMStatus:
    running = 'RAM\nDisk'
    busy = 'Busy'
    connect = 'Connect'
    reconnect = 'Reconnect'
    connected = 'Connected'
    allocating = 'Allocating'

    running_statuses = (running, busy)
    not_connected_statuses = (connect, reconnect)
    running_statuses_after_restart = (running, connected)


class ColabNotebook(WebPage):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._has_run_cell = False
        self._cell_options: Dict[int, CellOptions] = dict()

    @staticmethod
    def _css_selector_4_cell(cell_index: int):  # note it's 1 based index
        return f"div[id^=\"cell-\"]:nth-child({cell_index})"

    @staticmethod
    def _cell_icon_locator(cell_index: int) -> SeleniumLocator:  # note it's 1 based index
        return SeleniumLocator(By.CSS_SELECTOR,
                               f"{ColabNotebook._css_selector_4_cell(cell_index)} div.cell-execution paper-icon-button")

    @staticmethod
    def _connect_button_locator() -> Iterable[SeleniumLocator]:
        return SeleniumLocator(By.CSS_SELECTOR, "#top-toolbar colab-connect-button"), \
               SeleniumLocator(By.CSS_SELECTOR, "#connect")

    def get_cell_options(self, cell_index: int) -> CellOptions:
        options = self._cell_options.get(cell_index, None)
        if options is None:
            line = self.get_cell_top_line(cell_index)
            options = CellOptionParser.parse_line(line)
            self._cell_options[cell_index] = options
        return options

    def prepare(self):
        self._connect_vm()

    # def _get_button_4_cell(self, cell_index: int) -> Optional[WebElement]:
    #     # self._click_cell(cell_index)  it seems the cell icon is there even if not clicked
    #     try:
    #         return EC._find_element(self.driver, ColabNotebook._cell_icon_locator(cell_index))
    #     except selenium.common.exceptions.NoSuchElementException:
    #         return None

    def _get_cell(self, cell_index: int) -> Optional[WebElement]:
        try:
            return EC._find_element(self.driver, (By.CSS_SELECTOR, ColabNotebook._css_selector_4_cell(cell_index)))
        except selenium.common.exceptions.NoSuchElementException:
            return None

    def _get_cell_status(self, cell_index: int) -> Optional[CellStatus]:
        el_cell = self._get_cell(cell_index)
        if el_cell is None:
            print(f"Cell {cell_index} not found")
            return None
        else:
            cell_options = self.get_cell_options(cell_index)
            assert cell_options.stop_checking_seconds is None or cell_options.stop_checking_seconds > 0

            # if not given, the stop_checking_seconds of last cell is 120, 10 for other cells
            stop_checking_seconds = cell_options.stop_checking_seconds \
                if cell_options.stop_checking_seconds is not None \
                else 10 if self.cell_exists(cell_index + 1) else 120

            # when vm is busy, the cell status can be not running for a short time (have watched as long as 36 seconds)
            # so if the cell appears not running, need to check again and again to make sure it's really not running
            cell_status = None
            for seconds in range(0, stop_checking_seconds):
                # print(f"{time.asctime(time.gmtime())} Check cell {cell_index} status for {seconds} seconds")
                cell_status = get_cell_status(el_cell)
                if is_cell_status_stopped(cell_status):
                    time.sleep(1)
                else:
                    if seconds > 0:
                        print(f"{time.asctime(time.gmtime())} cell {cell_index} status is {cell_status}, "
                              f"but appeared stopped for {seconds} seconds")
                    return cell_status

            print(f"Cell {cell_index} is considered stopped: {cell_status}")
            return cell_status

    def get_cell_line_element(self, cell_index: int, line_number: int) -> Optional[WebElement]:
        cell_line_locator = \
            (By.CSS_SELECTOR,
             f"{ColabNotebook._css_selector_4_cell(cell_index)} div.view-line:nth-child({line_number})")
        retry_times = 0
        while True:
            try:
                el_line = EC.element_to_be_clickable(cell_line_locator)(self.driver)
                return el_line if el_line else None
            except selenium.common.exceptions.NoSuchElementException:
                return None
            except selenium.common.exceptions.StaleElementReferenceException:
                time.sleep(2)
                retry_times = retry_times + 1
                print(f"StaleElementReferenceException found when try to get line {line_number} of cell {cell_index}. "
                      f"Retry {retry_times}")

    def get_cell_line_text(self, cell_index: int, line_number: int) -> Optional[str]:
        el_line = self.get_cell_line_element(cell_index, line_number)
        return None if el_line is None else el_line.text

    def get_cell_top_line(self, cell_index: int) -> Optional[str]:
        line_number = 1
        while True:
            line_text = self.get_cell_line_text(cell_index, line_number)
            if line_text is None:
                return None
            elif re.fullmatch(r"\s*", line_text) is None:
                return line_text
            else:
                line_number = line_number + 1

    def get_cell_text(self, cell_index: int) -> Optional[List[str]]:
        if self.cell_exists(cell_index):
            result = list()
            line_number = 1
            while True:
                line_text = self.get_cell_line_text(cell_index, line_number)
                if line_text is None:
                    return result
                else:
                    result.append(line_text)
                    line_number = line_number + 1
        else:
            return None

    def _scroll_cell_into_view(self, cell_index: int):
        cell_locator = (By.CSS_SELECTOR, ColabNotebook._css_selector_4_cell(cell_index))

        try:
            el_cell = EC.visibility_of_element_located(cell_locator)(self.driver)
        except selenium.common.exceptions.NoSuchElementException:
            el_cell = None

        if el_cell:
            ActionChains(self.driver).move_to_element(el_cell).perform()
        else:
            raise selenium.common.exceptions.NoSuchElementException(f"Cell {cell_index} not found")

    def _click_cell(self, cell_index: int):
        el_cell = self._get_cell(cell_index)
        if el_cell is not None and not is_cell_focused(el_cell):
            line_number = 1
            last_err = None
            retry_times = 0
            while True:
                el_line = self.get_cell_line_element(cell_index, line_number)
                if el_line:
                    try:
                        ActionChains(self.driver).move_to_element(el_line).perform()
                        el_line.click()
                        break
                    except Exception as err:
                        last_err = err
                        line_number = line_number + 1
                else:
                    time.sleep(2)
                    retry_times = retry_times + 1
                    if retry_times > 3:
                        raise RuntimeError(f"failed to click cell {cell_index}: "
                                           f"{'line not found' if last_err is None else last_err}")
                    else:
                        if line_number > 1:
                            line_number = 1
            time.sleep(2)

    def stop_cell(self, cell_index: int):
        cell_status = self._get_cell_status(cell_index)
        if CellStatus.running == cell_status:
            self._click_cell(cell_index)
            self.js_click(ColabNotebook._cell_icon_locator(cell_index))
        elif CellStatus.pending == cell_status:
            raise RuntimeError(f"Cannot stop cell {cell_index} when it's waiting for vm")
        else:
            assert is_cell_status_stopped(cell_status)
            print(f"{self.title} - Cell {cell_index} has been stopped. Status: {cell_status}")

    def run_cell(self, cell_index: int):
        el_cell = self._get_cell(cell_index)
        if el_cell is not None and is_code_cell(el_cell):
            cell_options = self.get_cell_options(cell_index)
            print(f"{self.title} - run cell {cell_index} with option {cell_options}")

            if cell_options.mount_google_drive:
                self.mount_google_drive(cell_index)
            else:
                self._run_cell(cell_index)
                self.wait_4_cell_stop(cell_index)

    def _run_cell(self, cell_index: int):
        cell_status = self._get_cell_status(cell_index)
        if CellStatus.running == cell_status:
            print(f"Cell {cell_index} is already running occupying VM")
        elif CellStatus.pending == cell_status:
            print(f"Cell {cell_index} is already running, waiting for VM")
        else:
            assert is_cell_status_stopped(cell_status)
            # need to click the cell to ensure the run icon appears
            self._click_cell(cell_index)
            self.js_click(ColabNotebook._cell_icon_locator(cell_index))
            # self._get_button_4_cell(cell_index).click()

            if not self._has_run_cell:
                time.sleep(5)  # wait for the button doesn't move anymore.
                has_been_authorized = False
                try:
                    el_run_anyway = self.wait(10).until(EC.element_to_be_clickable((By.ID, "ok")))
                    print("Ask for authorization to run")
                except TimeoutException:
                    print("The notebook had been authorized to run")
                    has_been_authorized = True

                if not has_been_authorized:
                    self.wait(10).until(lambda _: el_run_anyway.text.strip().upper() == 'RUN ANYWAY')

                    print("Click RUN ANYWAY")
                    self.js_click((By.ID, "ok"))
                    try:
                        self.wait(10).until(EC.invisibility_of_element((By.ID, "ok")))
                    except TimeoutException as e:
                        print("Click RUN ANYWAY failed")
                        raise e

                    print("Authorized notebook to run")

                self._has_run_cell = True

            # do not wait for the cell turn to running status: it could end run quite quickly

    def wait_4_cell_stop(self, cell_index):
        notebook = self

        def wait_condition(driver):
            notebook.reconnect_vm_if_disconnected_message_pops_up()
            return is_cell_status_stopped(notebook._get_cell_status(cell_index))

        cell_options = self.get_cell_options(cell_index)
        self.wait(cell_options.max_run_seconds).until(wait_condition)

        cell_status = self._get_cell_status(cell_index)
        print(f"{self.title} - Cell {cell_index} stopped with {cell_status}")

        # if vm is gone during cell execution, the cell could still end without error
        # so if vm is gone after cell stops, consider the notebook is not done
        vm_status = self.get_vm_status()
        if vm_status in VMStatus.not_connected_statuses:
            print(f"{self.title} - VM status is {vm_status}. Try reconnect")
            self._connect_vm()

        if self.cell_exists(cell_index + 1):
            self.reconnect_vm_if_disconnected_message_pops_up()  # leave time for vm status text change after possible restart runtime
            try:
                self._wait_4_vm_running()
            except TimeoutException as err:
                err.msg = f"{err.msg}. Cell {cell_index} has already stopped"
                raise err

    def _get_el_connect_button(self):
        return self.wait().until(EC.element_to_be_clickable(ColabNotebook._connect_button_locator()))

    def _wait_4_vm_running(self, seconds: int = None, statuses: Iterable[str] = (VMStatus.running,)):
        el_connect = self._get_el_connect_button()
        try:
            self.wait(seconds).until(lambda d: el_connect.text.strip() in statuses)
        except TimeoutException as err:
            err.msg = f"{err.msg}. VM status: {el_connect.text.strip()}"
            raise err

    def get_vm_status(self):
        el_connect = self._get_el_connect_button()
        return el_connect.text.strip()

    def _check_failed_to_assign_backend(self):
        el_gpu_not_available_locator = SeleniumLocator(By.CSS_SELECTOR, "body colab-dialog paper-dialog")
        try:
            el_gpu_not_available = EC._find_element(self.driver, el_gpu_not_available_locator)
            if el_gpu_not_available is not None:
                el_gpu_not_available_text = el_gpu_not_available.text
                if el_gpu_not_available_text is not None and \
                        el_gpu_not_available_text.find("Failed to assign a backend") >= 0:
                    raise BackendNotAvailableError()
        except selenium.common.exceptions.NoSuchElementException:
            pass

    def _connect_vm(self):  # return VM status text
        print("Connecting VM")
        # click the connect button if it's there
        # if the VM has been allocated for this account, then colab will auto connect it
        el_connect = self._get_el_connect_button()

        # Wait until something is shown here
        self.wait().until(lambda driver: len(el_connect.text.strip()) > 0)

        connect_text = el_connect.text.strip()
        print(f"{self.title} - The status is {connect_text}")

        def handle_retry(curr_retry_times: int) -> int:
            if curr_retry_times > 10:
                raise RuntimeError(f"Connect VM failed {curr_retry_times} times. Give up")
            else:
                curr_retry_times = curr_retry_times + 1
                print(f"{self.title} - Connect VM failed. Retry {curr_retry_times}")
                time.sleep(5)
                return curr_retry_times

        retry_times = 0
        while True:
            if connect_text in VMStatus.not_connected_statuses:
                self.js_click(ColabNotebook._connect_button_locator())

            try:
                self._wait_4_vm_running(seconds=120, statuses=VMStatus.running_statuses)
                break
            except TimeoutException as e:
                connect_text = el_connect.text.strip()
                if connect_text in VMStatus.not_connected_statuses:
                    retry_times = handle_retry(retry_times)
                else:
                    if connect_text == VMStatus.allocating:
                        self._check_failed_to_assign_backend()
                    raise RuntimeError(f"Unexpected VM status: {connect_text}")

    def reconnect_vm_if_disconnected_message_pops_up(self):
        # check if there is an OK button whose title is RECONNECT
        try:
            el_ok = self.wait(10).until(EC.element_to_be_clickable((By.ID, "ok")))
            found_ok_button = True
        except TimeoutException:
            found_ok_button = False

        if found_ok_button and "RECONNECT" == el_ok.text.strip().upper():
            from datetime import datetime
            print(f"{self.title} - RECONNECT dialog found at {datetime.now()}")
            self.js_click((By.ID, "ok"))
            print(f"{self.title} - Clicked RECONNECT at {datetime.now()}")
            try:
                self._wait_4_vm_running()
                print(f"{self.title} - VM back to running at {datetime.now()}")
            except TimeoutException:
                print(f"{self.title} - VM failed back to running at {datetime.now()}")
                self._check_failed_to_assign_backend()

    def mount_google_drive(self, cell_index: int):
        retry_times = 0
        while True:

            self._run_cell(cell_index)

            time.sleep(10)  # wait for the iframe to be fully loaded, this is required in release mode

            # don't know why but it is a must for attaching driver to reattach to get content inside iframe
            self._recreate_driver()

            switched_to_frame = False
            timeout_happened = False

            try:
                css_selector_4_auth_iframe = \
                    f"{ColabNotebook._css_selector_4_cell(cell_index)} div.output-iframe-container iframe"
                self.wait().until(EC.frame_to_be_available_and_switch_to_it((By.CSS_SELECTOR, css_selector_4_auth_iframe)))

                switched_to_frame = True

                # check whether the authorization has been done
                output_body = self.wait().until(EC.presence_of_element_located((By.CSS_SELECTOR, "#output-body")))
                output_body_text = output_body.text

                if all(map(lambda txt: -1 == output_body_text.find(txt), ['Drive already mounted', 'Mounted at /gdrive'])):
                    print("Try authorize google drive")
                    auth_url = self.wait().until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, "#output-footer a"))).get_property('href')

                    print(f"auth url extracted: {auth_url}")
                    authorization_code = self._get_gdrive_authorization_code(auth_url)

                    print(f"authorization code get: {authorization_code}")
                    self.wait().until(EC.frame_to_be_available_and_switch_to_it((By.CSS_SELECTOR, css_selector_4_auth_iframe)))

                    print("send authorization code")
                    el_auth_code_input = self.wait().until(EC.presence_of_element_located(
                        (By.CSS_SELECTOR, "#output-footer input")))
                    el_auth_code_input.click()  # it fails sometimes if not click first
                    el_auth_code_input.send_keys(authorization_code, Keys.ENTER)

                    print("waiting for mount result")
                    self.wait().until(lambda driver: output_body.text.find('Mounted at /gdrive') != -1)
                    print("Google drive mounted")
                else:
                    try:
                        print(output_body_text)
                    except Exception:
                        print("Google drive already mounted")

            except selenium.common.exceptions.TimeoutException as err:
                timeout_happened = True
            finally:
                if switched_to_frame:
                    self.driver.switch_to.default_content()

            if timeout_happened:
                retry_times = retry_times + 1
                print(f"{self.title} - Timeout when mount google drive. Retry {retry_times}")
                cell_status = self._get_cell_status(cell_index)
                if CellStatus.running == cell_status:
                    print(f"Stop cell {cell_index}")
                    self.js_click(ColabNotebook._cell_icon_locator(cell_index))
                    print(f"wait for cell {cell_index} stop")
                    self.wait_4_cell_stop(cell_index)
                elif CellStatus.pending == cell_status:
                    raise RuntimeError(f"Unexpected cell status: running waiting for VM of cell {cell_index} "
                                       f"when mounting google drive")
            else:
                break

        self.wait_4_cell_stop(cell_index)

    def _get_gdrive_authorization_code(self, auth_url: str):
        window_handle_before_open_new_window = self.driver.current_window_handle

        retry_times = 0
        while True:
            try:
                self.driver.execute_script("window.open('about:blank', 'tab_authorization');")
                self.driver.switch_to.window("tab_authorization")
                self.driver.get(auth_url)

                # click to select google account
                self.wait().until(EC.element_to_be_clickable((By.ID, "profileIdentifier"))).click()

                # click the approve button
                self.wait().until(EC.element_to_be_clickable((By.ID, "submit_approve_access"))).click()

                # get the authorization code
                el_authorization_code = self.wait().until(EC.presence_of_element_located(
                    (By.CSS_SELECTOR, "#view_container div[data-form-action-uri] textarea")))
                authorization_code = el_authorization_code.text
                return authorization_code
            except selenium.common.exceptions.TimeoutException as err:
                retry_times = retry_times + 1
                if retry_times < 10:
                    print(f"Time out when get google driver authorization code. Retry {retry_times}")
                else:
                    raise RuntimeError(f"Time out when get google driver authorization code, retried {retry_times} times")
            finally:
                self.driver.close()
                self.driver.switch_to.window(window_handle_before_open_new_window)

    def cell_exists(self, cell_index: int) -> bool:
        return self._get_cell(cell_index) is not None

    def get_current_running_cell_index(self) -> Optional[int]:
        cell_index = 1
        while True:
            try:
                cell_status = self._get_cell_status(cell_index)
                if cell_status is None:
                    return None  # cell index out of boundary, no more cell
                elif CellStatus.running == cell_status:
                    return cell_index
                else:
                    cell_index = cell_index + 1
            except TimeoutException:
                raise RuntimeError("Time out when checking cell status, please make sure chrome zoom is set to 100%")

    def restart_runtime(self):
        print(f"{self.title} - restart runtime")
        # click the restart runtime menu item
        el_runtime_menu_locator = (By.CSS_SELECTOR, "#runtime-menu-button div.goog-inline-block.goog-menu-button-caption")
        el_runtime_menu = self.wait().until(EC.element_to_be_clickable(el_runtime_menu_locator))
        el_runtime_menu.click()
        print(f"{self.title} - runtime menu clicked")

        el_restart_menu_item_locator = (By.CSS_SELECTOR, "#runtime-menu div.goog-menuitem[command=restart] > div")
        el_restart_menu_item = self.wait().until(EC.element_to_be_clickable(el_restart_menu_item_locator))
        el_restart_menu_item.click()
        print(f"{self.title} - restart menu item clicked")

        time.sleep(10)  # wait for the button doesn't move anymore. this is required in release mode
        self.js_click((By.ID, "ok"))
        print(f"{self.title} - runtime restart confirmed")
        self._wait_4_vm_running(60 * 5, VMStatus.running_statuses_after_restart)
        print(f"{self.title} - runtime restarted")

    def factory_reset_runtime(self):
        print(f"{self.title} - factory reset runtime")
        # click the factory reset runtime menu item
        el_runtime_menu_locator = \
            (By.CSS_SELECTOR, "#runtime-menu-button div.goog-inline-block.goog-menu-button-caption")
        el_runtime_menu = self.wait().until(EC.element_to_be_clickable(el_runtime_menu_locator))
        el_runtime_menu.click()
        print(f"{self.title} - runtime menu clicked")

        el_menu_item_locator = (By.CSS_SELECTOR, "#runtime-menu div.goog-menuitem[command=powerwash-current-vm] > div")
        el_menu_item = self.wait().until(EC.element_to_be_clickable(el_menu_item_locator))
        el_menu_item.click()
        print(f"{self.title} - factory reset runtime menu item clicked")

        time.sleep(10)  # wait for the button doesn't move anymore. this is required in release mode
        self.js_click((By.ID, "ok"))  # this still works even if the dialog is in shadow DOM
        print(f"{self.title} - factory reset runtime confirmed")
        self._wait_4_vm_running(60 * 5, (VMStatus.reconnect,))
        print(f"{self.title} - factory reset runtime done")
        self._connect_vm()
