use scraper::{ElementRef, Html, Selector};

fn collect_text(element: ElementRef<'_>) -> String {
    let mut text = String::new();
    for part in element.text() {
        if !text.is_empty() {
            text.push(' ');
        }
        text.push_str(part);
    }

    text
}

pub fn select_all_in(document: &Html, selector: &str) -> Vec<String> {
    let Ok(selector) = Selector::parse(selector) else {
        return Vec::new();
    };

    document.select(&selector).map(collect_text).collect()
}

pub fn select_all(html: &str, selector: &str) -> Vec<String> {
    let document = Html::parse_document(html);
    select_all_in(&document, selector)
}

pub fn select_first(html: &str, selector: &str) -> Option<String> {
    let document = Html::parse_document(html);
    select_first_in(&document, selector)
}

pub fn select_first_in(document: &Html, selector: &str) -> Option<String> {
    let Ok(selector) = Selector::parse(selector) else {
        return None;
    };
    document.select(&selector).next().map(collect_text)
}

pub fn select_html(html: &str, selector: &str) -> Vec<String> {
    let document = Html::parse_document(html);
    select_html_in(&document, selector)
}

pub fn select_html_in(document: &Html, selector: &str) -> Vec<String> {
    let Ok(selector) = Selector::parse(selector) else {
        return Vec::new();
    };
    document.select(&selector).map(|el| el.html()).collect()
}
