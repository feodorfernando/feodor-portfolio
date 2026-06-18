// Homepage interactions: typing effect, scroll reveal, footer year.

// 1) typed rotating phrases in the hero
(function typed() {
  const el = document.getElementById('typed');
  if (!el) return;
  const phrases = [
    'real-time lakehouses',
    'Azure data pipelines',
    'Iceberg & Delta tables',
    'GenAI into the data stack',
    'Databricks Genie analytics',
  ];
  let pi = 0, ci = 0, deleting = false;
  function tick() {
    const word = phrases[pi];
    el.textContent = word.slice(0, ci);
    if (!deleting && ci < word.length) {
      ci++;
    } else if (!deleting && ci === word.length) {
      deleting = true;
      return setTimeout(tick, 1500);
    } else if (deleting && ci > 0) {
      ci--;
    } else {
      deleting = false;
      pi = (pi + 1) % phrases.length;
    }
    setTimeout(tick, deleting ? 45 : 80);
  }
  tick();
})();

// 2) reveal-on-scroll
(function reveal() {
  const els = document.querySelectorAll('.reveal');
  if (!('IntersectionObserver' in window) || !els.length) {
    els.forEach((e) => e.classList.add('in'));
    return;
  }
  const io = new IntersectionObserver(
    (entries) => {
      entries.forEach((en) => {
        if (en.isIntersecting) { en.target.classList.add('in'); io.unobserve(en.target); }
      });
    },
    { threshold: 0.12 }
  );
  els.forEach((e) => io.observe(e));
})();

// 3) footer year
(function year() {
  const y = document.getElementById('year');
  if (y) y.textContent = new Date().getFullYear();
})();
