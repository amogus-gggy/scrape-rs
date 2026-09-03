//! HTML querying helpers.
//!
//! [`Doc`] parses a page **once** and lets you drill into it with [`Node`]s that
//! borrow from that single parse. Compiled selectors are cached per thread, so
//! a scraper that reuses a handful of selectors over thousands of pages pays
//! the CSS parsing cost once per thread instead of once per call.
//!
//! The free functions ([`select_all`], [`select_first`], [`select_html`]) are
//! kept for convenience and one-off queries; they parse the document on every
//! call, so prefer [`Doc`] when you query the same page more than once.

use scraper::{ElementRef, Html, Selector};
use std::cell::RefCell;
use std::collections::HashMap;

thread_local! {
    /// Compiled-selector cache. Thread-local instead of a shared map so that
    /// pool workers never contend on a lock just to look up a selector.
    /// Invalid selectors are cached as `None` so they are not re-parsed either.
    static SELECTORS: RefCell<HashMap<String, Option<Selector>>> = RefCell::new(HashMap::new());
}

/// Calls `f` with the compiled `selector`, or returns `None` if it is invalid.
///
/// `f` must not call back into this function: the cache borrow is held for the
/// duration of the call. Every caller in this module fully materializes its
/// result inside `f`, so no user code runs while the borrow is active.
fn with_selector<R>(selector: &str, f: impl FnOnce(&Selector) -> R) -> Option<R> {
    SELECTORS.with(|cache| {
        let mut cache = cache.borrow_mut();
        if !cache.contains_key(selector) {
            cache.insert(selector.to_owned(), Selector::parse(selector).ok());
        }
        cache[selector].as_ref().map(f)
    })
}

/// Concatenates the text nodes of `el`, matching the historical join-with-space
/// behaviour of [`select_all`].
fn text_of_element(el: ElementRef<'_>) -> String {
    el.text().collect::<Vec<_>>().join(" ")
}

/// A parsed HTML document. Parse once, query many times.
///
/// ```
/// use scrape_rs::parsers::Doc;
///
/// let html = r#"<div class="quote"><span class="text">Aaa</span><a class="tag">x</a></div>"#;
/// let doc = Doc::parse(html);
/// for quote in doc.select(".quote") {
///     assert_eq!(quote.text_of(".text").as_deref(), Some("Aaa"));
///     assert_eq!(quote.texts_of(".tag"), vec!["x"]);
/// }
/// ```
pub struct Doc {
    html: Html,
}

impl Doc {
    pub fn parse(html: &str) -> Self {
        Self {
            html: Html::parse_document(html),
        }
    }

    /// All elements matching `selector`. An invalid selector yields an empty
    /// vector (same as the free functions).
    pub fn select(&self, selector: &str) -> Vec<Node<'_>> {
        with_selector(selector, |sel| self.html.select(sel).map(Node).collect()).unwrap_or_default()
    }

    /// The first element matching `selector`.
    pub fn first(&self, selector: &str) -> Option<Node<'_>> {
        with_selector(selector, |sel| self.html.select(sel).next().map(Node)).flatten()
    }

    /// Text of the first element matching `selector`.
    pub fn text_of(&self, selector: &str) -> Option<String> {
        self.first(selector).map(|node| node.text())
    }

    /// Text of every element matching `selector`.
    pub fn texts_of(&self, selector: &str) -> Vec<String> {
        self.select(selector)
            .into_iter()
            .map(|n| n.text())
            .collect()
    }
}

/// An element inside a [`Doc`]. Sub-queries reuse the parent's parse — nothing
/// is re-serialized or re-parsed while drilling down.
#[derive(Clone, Copy)]
pub struct Node<'a>(ElementRef<'a>);

impl<'a> Node<'a> {
    /// Descendants of this element matching `selector`.
    pub fn select(&self, selector: &str) -> Vec<Node<'a>> {
        with_selector(selector, |sel| self.0.select(sel).map(Node).collect()).unwrap_or_default()
    }

    /// First descendant matching `selector`.
    pub fn first(&self, selector: &str) -> Option<Node<'a>> {
        with_selector(selector, |sel| self.0.select(sel).next().map(Node)).flatten()
    }

    /// Text of the first descendant matching `selector`.
    pub fn text_of(&self, selector: &str) -> Option<String> {
        self.first(selector).map(|node| node.text())
    }

    /// Text of every descendant matching `selector`.
    pub fn texts_of(&self, selector: &str) -> Vec<String> {
        self.select(selector)
            .into_iter()
            .map(|n| n.text())
            .collect()
    }

    /// All text nodes of this element, joined by a space.
    pub fn text(&self) -> String {
        text_of_element(self.0)
    }

    /// Value of the `name` attribute, if present.
    pub fn attr(&self, name: &str) -> Option<&'a str> {
        self.0.value().attr(name)
    }

    /// Outer HTML of this element.
    pub fn html(&self) -> String {
        self.0.html()
    }

    /// Inner HTML of this element.
    pub fn inner_html(&self) -> String {
        self.0.inner_html()
    }
}

/// Text of every element matching `selector`.
///
/// Parses `html` on every call — use [`Doc`] for repeated queries on one page.
pub fn select_all(html: &str, selector: &str) -> Vec<String> {
    Doc::parse(html).texts_of(selector)
}

/// Text of the first element matching `selector`.
///
/// Parses `html` on every call — use [`Doc`] for repeated queries on one page.
pub fn select_first(html: &str, selector: &str) -> Option<String> {
    Doc::parse(html).text_of(selector)
}

/// Outer HTML of every element matching `selector`.
///
/// Parses `html` on every call — use [`Doc`] for repeated queries on one page.
pub fn select_html(html: &str, selector: &str) -> Vec<String> {
    Doc::parse(html)
        .select(selector)
        .into_iter()
        .map(|n| n.html())
        .collect()
}
