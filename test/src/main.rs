use scrape_rs::init_worker_pool;
use scrape_rs::parsers::{select_all, select_first, select_html};
use std::num::NonZeroUsize;

#[derive(Debug)]
#[allow(unused)]
struct Quote {
    text: String,
    author: String,
    tags: Vec<String>,
}

fn parse_quotes(html: &str) -> Vec<Quote> {
    let document = Html::parse_document(html);
    let quote_selector = Selector::parse(".quote").unwrap();
    let text_selector = Selector::parse(".text").unwrap();
    let author_selector = Selector::parse(".author").unwrap();
    let tag_selector = Selector::parse(".tag").unwrap();

    document
        .select(&quote_selector)
        .map(|quote| Quote {
            text: quote
                .select(&text_selector)
                .next()
                .map(|element| element.text().collect::<Vec<_>>().join(" "))
                .unwrap_or_default(),
            author: quote
                .select(&author_selector)
                .next()
                .map(|element| element.text().collect::<Vec<_>>().join(" "))
                .unwrap_or_default(),
            tags: quote
                .select(&tag_selector)
                .map(|element| element.text().collect::<Vec<_>>().join(" "))
                .collect(),
        })
        .collect()
}

fn main() {
    let urls: Vec<String> = (1..=100)
        .map(|i| format!("https://quotes.toscrape.com/page/{}", i))
        .collect();

    let pool = init_worker_pool(NonZeroUsize::new(1).unwrap()).unwrap();
    let fetch_handle = pool.handle();
    println!("threads started");

    for url in urls {
        pool.push(url);

    }
    pool.close(); // Signal that workers can finish, and that there will not be any new task.

    // Keep polling for partial results and parse them as soon as they arrive
    loop {
        for (_, res) in fetch_handle.ready_results() {
            match res {
                Ok(html) => {
                    let quotes = parse_quotes(&html);
                    for quote in quotes {
                        println!("{:?}", quote);
                    }
                }
                Err(e) => eprintln!("fetch error: {e}"),
            }
        }

        if fetch_handle.is_finished() {
            break;
        }

        // Avoid wasting CPU — short sleep between polls
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    include!("tests.rs");
}
