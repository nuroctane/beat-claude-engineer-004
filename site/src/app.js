// Behavior only. Rows are native <details>, so the page opens, reads and
// prints without this; it adds keyboard moves, filters, deep links and copy.
(() => {
  const ledger = document.querySelector('.ledger');
  if (!ledger) return;
  const rows = [...ledger.querySelectorAll('.row')];
  const heads = rows.map((r) => r.querySelector('summary'));
  const status = document.getElementById('status');
  const reduced = matchMedia('(prefers-reduced-motion: reduce)');
  const say = (text) => { if (status) status.textContent = text; };
  const detailsOf = (row) => row.querySelector('details');

  // A keyboard toggle opens instantly; a pointer toggle gets the short settle.
  ledger.addEventListener('pointerdown', (e) => {
    e.target.closest('.row')?.classList.remove('instant');
  });

  ledger.addEventListener('keydown', (e) => {
    const i = heads.indexOf(document.activeElement);
    if (i < 0) return;
    if (e.key === 'Enter' || e.key === ' ') {
      rows[i].classList.add('instant');
      return;
    }
    if (e.key === 'Escape') {
      detailsOf(rows[i]).open = false;
      return;
    }
    const to = { ArrowDown: i + 1, ArrowUp: i - 1, Home: 0, End: heads.length - 1 }[e.key];
    if (to === undefined) return;
    e.preventDefault();
    heads[Math.max(0, Math.min(heads.length - 1, to))].focus();
  });

  // Filters dim; they never reorder or hide.
  const chips = [...document.querySelectorAll('.chip')];
  chips.forEach((chip) => chip.addEventListener('click', () => {
    const f = chip.dataset.filter;
    chips.forEach((c) => c.setAttribute('aria-pressed', String(c === chip)));
    ledger.dataset.filter = f;
    const n = f === 'all' ? rows.length : rows.filter((r) => r.dataset.actions.split(' ').includes(f)).length;
    say(f === 'all' ? `Showing all ${rows.length} lines.` : `${n} of ${rows.length} lines: ${f}. The rest are dimmed.`);
  }));

  // Jump to a line: from the face-value list or a #L<n> link someone shared.
  function land(id, { focus = true } = {}) {
    const row = document.getElementById(id);
    if (!row || !row.classList.contains('row')) return false;
    if (ledger.dataset.filter !== 'all') chips[0].click();
    row.classList.add('instant');
    detailsOf(row).open = true;
    // 'start' plus html's scroll-padding parks it right under the sticky filters.
    // Smooth scroll rides rAF, which a background tab pauses, so jump when hidden.
    const smooth = !reduced.matches && !document.hidden;
    row.scrollIntoView({ behavior: smooth ? 'smooth' : 'auto', block: 'start' });
    row.classList.remove('landed');
    void row.offsetWidth;
    row.classList.add('landed');
    if (focus) heads[rows.indexOf(row)].focus({ preventScroll: true });
    return true;
  }
  document.addEventListener('click', (e) => {
    const a = e.target.closest('a[href^="#L"]');
    if (!a) return;
    const id = a.getAttribute('href').slice(1);
    if (land(id)) {
      e.preventDefault();
      history.replaceState(null, '', `#${id}`);
    }
  });
  const fromHash = () => /^#L\d+$/.test(location.hash) && land(location.hash.slice(1), { focus: false });
  fromHash();
  addEventListener('hashchange', fromHash);

  // Printing shows every line open, then puts the page back the way it was.
  let wasOpen = [];
  addEventListener('beforeprint', () => {
    wasOpen = rows.map((r) => detailsOf(r).open);
    rows.forEach((r) => { detailsOf(r).open = true; });
  });
  addEventListener('afterprint', () => {
    rows.forEach((r, i) => { detailsOf(r).open = wasOpen[i] ?? false; });
  });

  // Copy commands.
  document.querySelectorAll('.copy').forEach((btn) => {
    let timer;
    btn.addEventListener('click', async () => {
      const source = document.getElementById(btn.dataset.copy);
      if (!source) return;
      let ok = true;
      try {
        await navigator.clipboard.writeText(source.textContent);
      } catch {
        // Clipboard can be blocked (iframes, permissions). Select the text instead.
        ok = false;
        const range = document.createRange();
        range.selectNodeContents(source);
        getSelection().removeAllRanges();
        getSelection().addRange(range);
      }
      btn.querySelector('.copy-b').textContent = ok ? 'copied' : 'selected';
      btn.classList.add('done');
      say(ok ? 'Commands copied.' : 'Commands selected. Press Ctrl+C to copy.');
      clearTimeout(timer);
      timer = setTimeout(() => btn.classList.remove('done'), 1600);
    });
  });
})();
