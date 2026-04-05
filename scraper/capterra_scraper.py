from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import pandas as pd
import time

PRODUCTS = {
    "Vision Helpdesk": "https://www.capterra.com/p/67930/Vision-Helpdesk/reviews/",
    "Freshdesk":        "https://www.capterra.com/p/113240/Freshdesk/reviews/",
    "Zendesk":          "https://www.capterra.com/p/65802/Zendesk-Support-Suite/reviews/",
    "Zoho Desk":        "https://www.capterra.com/p/138208/Zoho-Desk/reviews/",
}

def get_driver():
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36")
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    return driver

def scrape_product(driver, product_name, url):
    print(f"🔄 Scraping: {product_name}")
    reviews = []

    for page in range(1, 6):
        paged_url = url + f"?page={page}"
        driver.get(paged_url)
        time.sleep(3)

        cards = driver.find_elements(By.CSS_SELECTOR, "[data-testid='review-card'], .review-card, article")

        if not cards:
            print(f"  ⚠️ No cards found on page {page}, stopping.")
            break

        for card in cards:
            try:
                text = card.find_element(By.CSS_SELECTOR, "p").text
                rating_el = card.find_elements(By.CSS_SELECTOR, "[aria-label*='out of']")
                rating = rating_el[0].get_attribute("aria-label").split()[0] if rating_el else None

                reviews.append({
                    "complaint_text": text,
                    "rating": rating,
                    "date": None,
                    "source": "capterra",
                    "product": product_name,
                    "company_size": None,
                    "user_id": None
                })
            except:
                continue

        print(f"  ✅ Page {page}: {len(cards)} cards found")
        time.sleep(2)

    return reviews

driver = get_driver()
all_reviews = []

for product, url in PRODUCTS.items():
    reviews = scrape_product(driver, product, url)
    all_reviews.extend(reviews)

driver.quit()

df = pd.DataFrame(all_reviews)
df.to_csv("data/capterra_reviews.csv", index=False)
print(f"\n✅ Saved {len(df)} reviews to data/capterra_reviews.csv")
print(df.head())