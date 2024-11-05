from utils.workflows.govt_sites.sites import bea, bls, cbo, fed_beige, ny_fed, fed, treasury, white_house

site_parsers = {
    "bea": {
        "name": "Bureau of Economic Analysis",
        "link": "https://www.bea.gov",
        "links_parse": bea.parse_bea_blog_articles,
        "article_parse": bea.parse_bea_article
    },
    "bls": {
        "name": "Bureau of Labor Statistics",
        "link": "https://www.bls.gov/bls/newsrels.htm#latest-releases",
        "links_parse": bls.parse_bls_news_releases,
        "article_parse": bls.parse_bls_article_content
    },
    "cbo": {
        "name": "Congressional Budget Office",
        "link": "https://www.cbo.gov/data/publications-with-data-files",
        "links_parse": cbo.parse_cbo_article_links,
        "article_parse": cbo.parse_cbo_article_content
    },
    "fedbeige": {
        "name": "Federal Reserve Beige Book",
        "link": "https://www.federalreserve.gov/monetarypolicy/publications/beige-book-default.htm",
        "links_parse": fed_beige.parse_fed_beige_book_links,
        "article_parse": fed_beige.parse_beige_book_article
    },
    "nyfed": {
        "name": "Federal Reserve Bank of New York",
        "link": "https://www.newyorkfed.org/press",
        "links_parse": ny_fed.parse_ny_fed_news_links,
        "article_parse": ny_fed.parse_ny_fed_article_content
    },
    "fed": {
        "name": "Federal Reserve",
        "link": "https://www.federalreserve.gov",
        "links_parse": fed.parse_fed_recent_developments,
        "article_parse": fed.parse_fed_article_content
    },
    "treasury": {
        "name": "U.S. Department of the Treasury",
        "link": "https://home.treasury.gov/news/press-releases",
        "links_parse": treasury.parse_treasury_press_release_links,
        "article_parse": treasury.parse_treasury_article_content
    },
    "wh": {
        "name": "The White House",
        "link": "https://www.whitehouse.gov/briefing-room/",
        "links_parse": white_house.parse_white_house_news_links,
        "article_parse": white_house.parse_white_house_article_content
    }
}