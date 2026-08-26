use scrape_rs::init_worker_pool;
use scrape_rs::parsers::Doc;
use std::num::NonZeroUsize;

#[derive(Debug)]
#[allow(unused)]
struct Quote {
    text: String,
    author: String,
    tags: Vec<String>,
}

/// Parses the page once and reads every field off that single parse.
fn parse_quotes(html: &str) -> Vec<Quote> {
    Doc::parse(html)
        .select(".quote")
        .into_iter()
        .map(|quote| Quote {
            text: quote.text_of(".text").unwrap_or_default(),
            author: quote.text_of(".author").unwrap_or_default(),
            tags: quote.texts_of(".tag"),
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

    // Parse pages as soon as they arrive. wait_ready blocks until there is
    // something to do, so there is no polling interval to tune.
    loop {
        for (_, res) in fetch_handle.wait_ready() {
            match res {
                Ok(html) => {
                    for quote in parse_quotes(&html) {
                        println!("{:?}", quote);
                    }
                }
                Err(e) => eprintln!("fetch error: {e}"),
            }
        }

        if fetch_handle.is_finished() {
            break;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    include!("tests.rs");
}
