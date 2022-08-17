from concurrent.futures import ThreadPoolExecutor, as_completed
import mechanize
import pandas as pd
from bs4 import BeautifulSoup
import regex as re
import requests
from tqdm import tqdm
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
from webdriver_manager.chrome import ChromeDriverManager
from selenium.common.exceptions import NoSuchElementException
import sys
import os


def fetch_data(url):
    try:
        soup = BeautifulSoup(
            requests.get(url).content,
            "html.parser",
        )

        purpose = (
            soup.find("td", text="Application Purpose").find_next_sibling().text.strip()
        )
        if purpose == "AM - Amendment":
            purpose = (
                soup.find("td", text="Original Application Purpose")
                .find_next_sibling()
                .text.strip()
            )

        if purpose == "NE - New":
            info = [""] * 8

            info[1] = (
                soup.find("td", text="File Number").find_next_sibling().text.strip()
            )
            info[2] = (
                soup.find("td", text="Application Status")
                .find_next_sibling()
                .text.strip()
            )
            info[3] = (
                soup.find("td", text="Receipt Date").find_next_sibling().text.strip()
            )
            info[4] = (
                soup.find("td", text="Action Date").find_next_sibling().text.strip()
            )

            info[5] = (
                soup.find("td", text="Name")
                .find_next_sibling()
                .text.strip()
                .split("\n")[0]
            )
            info[6] = (
                soup.find("td", text="FRN")
                .find_next_sibling()
                .text.strip()
                .split("\n")[0]
            )

            market_tab = soup.find("a", title="Market")
            market_soup = BeautifulSoup(
                requests.get(
                    "https://wireless2.fcc.gov/UlsApp/ApplicationSearch/"
                    + market_tab.get("href")
                ).content,
                "html.parser",
            )

            info[0] = (
                re.compile(r"\d{5}")
                .search(market_soup.find("a", title="Market Detail").text.strip())
                .group(0)
            )
            try:
                info[7] = market_soup.find(
                    "a", title="Link to License in new window"
                ).text.strip()
            except:
                pass

            return info

        elif purpose == "LN - New Lease":
            info = [""] * 13

            info[1] = (
                soup.find("td", text="File Number").find_next_sibling().text.strip()
            )
            info[2] = (
                soup.find("td", text="Application Status")
                .find_next_sibling()
                .text.strip()
            )
            info[3] = (
                soup.find("td", text="Receipt Date").find_next_sibling().text.strip()
            )
            info[4] = (
                soup.find("td", text="Action Date").find_next_sibling().text.strip()
            )

            info[11] = (
                soup.find("td", text="Classification of Lease")
                .find_next_sibling()
                .text.strip()
            )

            licensee_block = (
                soup.find("b", text="Licensee Information")
                .find_parent("td")
                .find_parent("tr")
            )
            info[5] = (
                licensee_block.find_next_sibling()
                .find_next_sibling()
                .find("td")
                .find_next_sibling()
                .text.strip()
                .split("\n")[0]
            )
            info[6] = (
                licensee_block.find_next_sibling()
                .find("td")
                .find_next_sibling()
                .text.strip()
                .split("\n")[0]
            )

            lessee_block = (
                soup.find("b", text="Lessee Information")
                .find_parent("td")
                .find_parent("tr")
            )
            info[7] = (
                lessee_block.find_next_sibling()
                .find_next_sibling()
                .find("td")
                .find_next_sibling()
                .text.strip()
                .split("\n")[0]
            )
            info[8] = (
                lessee_block.find_next_sibling()
                .find("td")
                .find_next_sibling()
                .text.strip()
                .split("\n")[0]
            )

            lease_tab = soup.find("a", title="Leases")
            lease_soup = BeautifulSoup(
                requests.get(
                    "https://wireless2.fcc.gov/UlsApp/ApplicationSearch/"
                    + lease_tab.get("href")
                ).content,
                "html.parser",
            )

            adjacent_to_fips = lease_soup.findAll(
                "td", text="BS - 900 MHz Broadband Service"
            )
            fips = []
            for block in adjacent_to_fips:
                fips.append(
                    re.compile(r"\d{5}")
                    .search(block.find_next_sibling().text.strip())
                    .group(0)
                )

            lic_links = lease_soup.findAll("a", title="Link to License in new window")
            call_signs = []
            for link in lic_links:
                call_signs.append(link.text.strip())

            leases = []
            try:
                lease_links = lease_soup.findAll(
                    "a", title="Link to new License in new window"
                )
                leases = []
                for lease_link in lease_links:
                    lease_id = lease_link.text.strip()
                    lease_page = BeautifulSoup(
                        requests.get(lease_link.get("href")).content,
                        "html.parser",
                    )
                    status = (
                        lease_page.find("td", text="Status")
                        .find_next_sibling()
                        .text.strip()
                    )
                    leases.append([lease_id, status])
            except:
                pass

            output = []
            for cs in call_signs:
                info[9] = cs
                output.append(info.copy())
            for i in range(len(output)):
                output[i][0] = fips[i]
                output[i][10] = leases[i][0]
                output[i][12] = leases[i][1]

            return tuple(output)

        else:
            info = [""] * 11

            info[1] = (
                soup.find("td", text="File Number").find_next_sibling().text.strip()
            )
            info[2] = (
                soup.find("td", text="Application Status")
                .find_next_sibling()
                .text.strip()
            )
            info[3] = (
                soup.find("td", text="Receipt Date").find_next_sibling().text.strip()
            )
            info[4] = (
                soup.find("td", text="Action Date").find_next_sibling().text.strip()
            )

            assignor_block = (
                soup.find("b", text="Assignor Information")
                .find_parent("td")
                .find_parent("tr")
            )
            info[5] = (
                assignor_block.find_next_sibling()
                .find_next_sibling()
                .find("td")
                .find_next_sibling()
                .text.strip()
                .split("\n")[0]
            )
            info[6] = (
                assignor_block.find_next_sibling()
                .find("td")
                .find_next_sibling()
                .text.strip()
                .split("\n")[0]
            )

            assignee_block = (
                soup.find("b", text="Assignee Information")
                .find_parent("td")
                .find_parent("tr")
            )
            info[7] = (
                assignee_block.find_next_sibling()
                .find_next_sibling()
                .find("td")
                .find_next_sibling()
                .text.strip()
                .split("\n")[0]
            )
            info[8] = (
                assignee_block.find_next_sibling()
                .find("td")
                .find_next_sibling()
                .text.strip()
                .split("\n")[0]
            )

            license_tab = soup.find("a", title="Licenses")
            license_soup = BeautifulSoup(
                requests.get(
                    "https://wireless2.fcc.gov/UlsApp/ApplicationSearch/"
                    + license_tab.get("href")
                ).content,
                "html.parser",
            )

            info[0] = (
                re.compile(r"\d{5}")
                .search(
                    license_soup.find("td", text="BS - 900 MHz Broadband Service")
                    .find_next_sibling()
                    .text.strip()
                )
                .group(0)
            )
            info[9] = license_soup.find(
                "a", title=re.compile(r"Link to License in new window")
            ).text.strip()
            try:
                info[10] = license_soup.find(
                    "a", title=re.compile(r"Link to new License in new window")
                ).text.strip()
            except:
                info[10] = ""

            return info

    except AttributeError as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(url, e, fname, exc_tb.tb_lineno)
        return []


def get_result():
    # Set up driver
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()), options=chrome_options
    )

    # Open search form
    driver.get("https://wireless2.fcc.gov/UlsApp/ApplicationSearch/searchAdvanced.jsp")

    # Fill out search form
    select = Select(driver.find_element(By.ID, "radioServiceCode"))
    select.select_by_value("BS")
    select = Select(driver.find_element(By.ID, "ulsStatus"))
    select.select_by_value("2")  # Pending
    select.select_by_value("A")  # A Granted
    select.select_by_value("C")  # Consented To
    select.select_by_value("G")  # Granted
    select.select_by_value("M")  # Consummated
    select.select_by_value("N")  # Granted in Part
    select.select_by_value("P")  # Pending Pack Filing
    select.select_by_value("Q")  # Accepted
    # select.select_by_value("I")  # Inactive
    select = Select(driver.find_element(By.ID, "ulsRowsPerPage"))
    select.select_by_value("100")
    select = Select(driver.find_element(By.ID, "ulsPurpose"))
    select.select_by_value("AA")  # Assignments
    select.select_by_value("LN")  # New Leases
    select.select_by_value("NE")  # New Licenses

    # Submit search form
    driver.find_element(By.ID, "ulsRowsPerPage").submit()

    # Get all application details
    elems = driver.find_elements(By.XPATH, "//a[@title='Application Details']")
    appl_links = [elem.get_attribute("href") for elem in elems]

    # Until no next page, do:
    while True:
        try:
            # Click next page
            driver.find_element(By.XPATH, "//a[@title='Next page of results']").click()

            # Get all application details
            elems = driver.find_elements(By.XPATH, "//a[@title='Application Details']")
            appl_links.extend([elem.get_attribute("href") for elem in elems])
        except NoSuchElementException:
            break

    data = []
    threads = []
    with tqdm(total=len(appl_links), desc="Downloading Info From Applications") as pbar:
        with ThreadPoolExecutor(max_workers=8) as executor:
            for link in appl_links:
                threads.append(executor.submit(fetch_data, link))
            for task in as_completed(threads):
                data.append(task.result())
                pbar.update(1)

    # Split up data[] according to application purpose
    new = []
    new_lease = []
    assign_auth = []
    for arr in data:
        if type(arr) == tuple:
            for sub_arr in arr:
                if len(sub_arr) == 8:
                    new.append(sub_arr)
                elif len(sub_arr) == 11:
                    assign_auth.append(sub_arr)
                elif len(sub_arr) == 13:
                    new_lease.append(sub_arr)
        elif len(arr) == 8:
            new.append(arr)
        elif len(arr) == 11:
            assign_auth.append(arr)
        elif len(arr) == 13:
            new_lease.append(arr)

    # Turn each arr into a dataframe, and send those frames to tabs in excel
    new_frame = pd.DataFrame(
        new,
        columns=[
            "County FIPS",  # 0
            "File Number",  # 1
            "Application Status",  # 2
            "Receipt Date",  # 3
            "Action Date",  # 4
            "Applicant Name",  # 5
            "Applicant FRN",  # 6
            "Call Sign",  # 7
        ],
    )
    new_lease_frame = pd.DataFrame(
        new_lease,
        columns=[
            "County FIPS",  # 0
            "File Number",  # 1
            "Application Status",  # 2
            "Receipt Date",  # 3
            "Action Date",  # 4
            "Licensee Name",  # 5
            "Licensee FRN",  # 6
            "Lessee Name",  # 7
            "Lessee FRN",  # 8
            "Call Sign",  # 9
            "New Lease ID",  # 10
            "Classification of Lease",  # 11
            "Lease Status",  # 12
        ],
    )
    assign_auth_frame = pd.DataFrame(
        assign_auth,
        columns=[
            "County FIPS",  # 0
            "File Number",  # 1
            "Application Status",  # 2
            "Receipt Date",  # 3
            "Action Date",  # 4
            "Assignor Name",  # 5
            "Assignor FRN",  # 6
            "Assignee Name",  # 7
            "Assignee FRN",  # 8
            "Call Sign",  # 9
            "New Call Sign",  # 10
        ],
    )

    writer = pd.ExcelWriter("Anterix_Applications_220817.xlsx", engine="xlsxwriter")
    new_frame.to_excel(writer, sheet_name="New Licenses", index=False)
    new_lease_frame.to_excel(writer, sheet_name="New Leases", index=False)
    assign_auth_frame.to_excel(
        writer, sheet_name="Assignments of Authorization", index=False
    )
    writer.save()


get_result()
