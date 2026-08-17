// Review Radar — highlights Amazon reviews written by people on your FRIENDS list.

const HIGHLIGHT_STYLE = `
  .rr-friend-review {
    outline: 3px solid #16a34a;
    outline-offset: 4px;
    border-radius: 6px;
  }
  .rr-friend-badge {
    display: inline-block;
    margin-left: 8px;
    padding: 2px 8px;
    background: #16a34a;
    color: #fff;
    font-size: 12px;
    font-weight: 700;
    border-radius: 999px;
    vertical-align: middle;
  }
`;

function normalize(name) {
  return name.toLowerCase().replace(/\s+/g, " ").trim();
}

function isFriend(reviewerName) {
  const n = normalize(reviewerName);
  return FRIENDS.some((f) => n === normalize(f));
}

function scanReviews() {
  let matches = 0;
  // Works on both the product page's review section and the
  // dedicated "all reviews" page.
  document.querySelectorAll('[data-hook="review"]').forEach((review) => {
    const nameEl = review.querySelector(".a-profile-name");
    if (!nameEl || review.classList.contains("rr-friend-review")) return;
    if (isFriend(nameEl.textContent)) {
      review.classList.add("rr-friend-review");
      const badge = document.createElement("span");
      badge.className = "rr-friend-badge";
      badge.textContent = "Friend";
      nameEl.insertAdjacentElement("afterend", badge);
      matches++;
    }
  });
  if (matches > 0) console.log(`[Review Radar] highlighted ${matches} friend review(s)`);
}

const style = document.createElement("style");
style.textContent = HIGHLIGHT_STYLE;
document.head.appendChild(style);

scanReviews();
// Amazon loads/paginates reviews dynamically; re-scan when the DOM changes.
new MutationObserver(() => scanReviews()).observe(document.body, {
  childList: true,
  subtree: true,
});
