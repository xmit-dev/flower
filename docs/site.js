"use strict";

// Every page remains complete and navigable without JavaScript.
document.documentElement.classList.add("js-enabled");

// Keep direct-file links at the canonical directory URL.
if (location.pathname.endsWith("/index.html")) {
  history.replaceState(null, "", `${location.pathname.slice(0, -10)}${location.search}${location.hash}`);
}

const menuButton = document.querySelector(".site-menu");
const siteNav = document.querySelector("#site-nav");
const sectionNav = document.querySelector("#section-nav");
const status = document.querySelector("#copy-status");
const mobile = window.matchMedia("(max-width: 820px)");
const copyTimers = new WeakMap();
const siteHeader = document.querySelector(".site-header");

// Navigation can take a second row on small screens or when text is enlarged.
// Keep anchors and the contents popover below its actual height.
function measureHeader() {
  if (siteHeader) document.documentElement.style.setProperty(
    "--site-header-height", `${Math.ceil(siteHeader.getBoundingClientRect().height)}px`,
  );
}
measureHeader();
window.addEventListener("resize", measureHeader);
if (siteHeader && typeof ResizeObserver !== "undefined") new ResizeObserver(measureHeader).observe(siteHeader);

// Scroll only the sidebar list; scrollIntoView would also move the article.
function revealInSidebar(link) {
  if (!link || !siteNav?.clientHeight) return;
  const viewport = siteNav.getBoundingClientRect();
  const entry = link.getBoundingClientRect();
  const top = viewport.top + 6;
  const bottom = viewport.bottom - 6;
  if (entry.top < top) siteNav.scrollTop -= Math.ceil(top - entry.top);
  else if (entry.bottom > bottom) siteNav.scrollTop += Math.ceil(entry.bottom - bottom);
}
function revealCurrentSection() {
  revealInSidebar(sectionNav?.querySelector('[aria-current="location"]') ?? siteNav?.querySelector('[aria-current="page"]'));
}
revealCurrentSection();

if (menuButton && siteNav) {
  function setMenu(open) {
    document.body.classList.toggle("nav-open", open);
    menuButton.setAttribute("aria-expanded", String(open));
    menuButton.querySelector("span").textContent = open ? "−" : "＋";
    if (open) revealCurrentSection();
  }
  menuButton.addEventListener("click", () => setMenu(menuButton.getAttribute("aria-expanded") !== "true"));
  siteNav.addEventListener("click", (event) => {
    if (event.target.closest("a")) setMenu(false);
  });
  document.addEventListener("keydown", (event) => {
    if (event.key === "Escape" && document.body.classList.contains("nav-open")) {
      setMenu(false);
      menuButton.focus();
    }
  });
  mobile.addEventListener("change", () => setMenu(false));
  document.addEventListener("click", (event) => {
    if (document.body.classList.contains("nav-open") &&
        !siteNav.contains(event.target) && !menuButton.contains(event.target)) setMenu(false);
  });
}

for (const button of document.querySelectorAll(".copy-button")) {
  const originalLabel = button.getAttribute("aria-label");
  button.addEventListener("click", async () => {
    const code = button.closest(".code-block").querySelector("code");
    let copied = false;
    try {
      if (navigator.clipboard && window.isSecureContext) {
        await navigator.clipboard.writeText(code.textContent);
        copied = true;
      } else {
        const field = document.createElement("textarea");
        field.value = code.textContent;
        field.setAttribute("readonly", "");
        field.style.position = "fixed";
        field.style.opacity = "0";
        document.body.append(field);
        try {
          field.select();
          copied = document.execCommand("copy");
        } finally {
          field.remove();
          button.focus({ preventScroll: true });
        }
      }
    } catch { /* Fall back to selecting the visible example. */ }
    clearTimeout(copyTimers.get(button));
    if (copied) {
      button.textContent = "Copied ✓";
      button.dataset.copied = "true";
      button.setAttribute("aria-label", "Code copied to clipboard");
      status.textContent = "Code copied to clipboard.";
      copyTimers.set(button, setTimeout(() => {
        button.textContent = "Copy";
        delete button.dataset.copied;
        button.setAttribute("aria-label", originalLabel);
      }, 2200));
    } else {
      const range = document.createRange();
      range.selectNodeContents(code);
      const selection = window.getSelection();
      selection.removeAllRanges();
      selection.addRange(range);
      status.textContent = "Clipboard access is unavailable. The code is selected; use your device’s copy command.";
    }
  });
}

if (sectionNav) {
  const chapters = [...document.querySelectorAll(".page-content > section[id]")];
  const navigation = [...sectionNav.querySelectorAll("a")];
  let scrollQueued = false;
  let previousSection;
  function markCurrentSection() {
    scrollQueued = false;
    // Use the same single offset as anchor scrolling. Combining root scroll
    // padding with chapter scroll margins used to leave the previous link lit.
    const anchorTop = parseFloat(getComputedStyle(document.documentElement).scrollPaddingTop) || 0;
    // Before the first section, the page intro is current: light nothing.
    let current = null;
    for (const chapter of chapters) {
      if (chapter.getBoundingClientRect().top <= anchorTop + 1) current = chapter.id;
    }
    for (const link of navigation) {
      if (link.hash === `#${current}`) link.setAttribute("aria-current", "location");
      else link.removeAttribute("aria-current");
    }
    // Follow document navigation without fighting someone browsing the list.
    if (current !== previousSection) revealCurrentSection();
    previousSection = current;
  }
  window.addEventListener("scroll", () => {
    if (!scrollQueued) {
      scrollQueued = true;
      window.requestAnimationFrame(markCurrentSection);
    }
  }, { passive: true });
  window.addEventListener("resize", () => {
    markCurrentSection();
    revealCurrentSection();
  });
  window.addEventListener("load", markCurrentSection);
  markCurrentSection();
}
