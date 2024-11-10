import puppeteer from "puppeteer"

const main = async () => {
    const browser = await puppeteer.launch({headless: false});
    const page = await browser.newPage();
  
    await page.goto('https://mvp.sos.ga.gov/s/voter-absentee-files?electionYear=2024&electionName=11%2F5%2F2024%20-%20NOVEMBER%205%2C%202024%20-%20GENERAL%20ELECTION&elecId=a0p3d00000LWdF5AAL&countyName=&name=A-12311&page=voterAbsenteeFilesDetails');
  
    const button = await page.locator('.slds-small-size_1-of-1 > p:nth-child(2) > a:nth-child(1)').setTimeout(6000);
    if (!button) {
        console.log("ERROR - no button");
        return;
    }

    await button.click()
  
    // close brower when we are done
    await browser.close();
}

main()
