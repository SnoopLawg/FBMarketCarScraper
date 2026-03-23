/**
 * Motion animations for AutoAlert UI.
 * Uses the Motion library (motion.dev) for spring physics and scroll-triggered animations.
 * Degrades gracefully if Motion fails to load — site works without animations.
 */

let animate, stagger, inView, spring, timeline;

async function initMotion() {
    try {
        const m = await import('https://cdn.jsdelivr.net/npm/motion@11/+esm');
        animate = m.animate;
        stagger = m.stagger;
        inView = m.inView;
        spring = m.spring;
        timeline = m.timeline;
        return true;
    } catch (e) {
        console.warn('[animations] Motion library failed to load, skipping animations');
        return false;
    }
}

// ── Spring presets ──
const springs = {
    snappy: { stiffness: 300, damping: 25 },
    bouncy: { stiffness: 200, damping: 15 },
    gentle: { stiffness: 150, damping: 20 },
    slow:   { stiffness: 100, damping: 20 },
};

// ── Page load entrance ──
function animatePageEntrance() {
    // Navbar fade down
    const nav = document.querySelector('.topnav');
    if (nav) {
        animate(nav,
            { opacity: [0, 1], y: [-10, 0] },
            { duration: 0.4, easing: spring(springs.snappy) }
        );
    }

    // Toolbar slide in
    const toolbar = document.querySelector('.toolbar');
    if (toolbar) {
        animate(toolbar,
            { opacity: [0, 1], y: [-8, 0] },
            { duration: 0.35, delay: 0.1, easing: spring(springs.snappy) }
        );
    }

    // Page content fade up
    const page = document.querySelector('.page');
    if (page) {
        animate(page,
            { opacity: [0, 1] },
            { duration: 0.3, delay: 0.05 }
        );
    }
}

// ── Deal card stagger entrance ──
function animateDealCards() {
    const cards = document.querySelectorAll('.deal-card');
    if (!cards.length) return;

    // Set initial state
    cards.forEach(card => {
        card.style.opacity = '0';
        card.style.transform = 'translateY(20px) scale(0.97)';
    });

    // Animate visible cards immediately
    const visibleCards = Array.from(cards).slice(0, 12);
    animate(visibleCards,
        { opacity: [0, 1], y: [20, 0], scale: [0.97, 1] },
        {
            delay: stagger(0.06, { start: 0.15 }),
            easing: spring(springs.gentle),
        }
    );

    // Animate remaining cards on scroll
    const remainingCards = Array.from(cards).slice(12);
    if (remainingCards.length) {
        inView(remainingCards, (info) => {
            animate(info.target,
                { opacity: [0, 1], y: [20, 0], scale: [0.97, 1] },
                { duration: 0.5, easing: spring(springs.gentle) }
            );
        }, { margin: '-50px' });
    }
}

// ── Stat cards (analytics page) ──
function animateStatCards() {
    const statCards = document.querySelectorAll('.stat-card');
    if (!statCards.length) return;

    statCards.forEach(card => {
        card.style.opacity = '0';
        card.style.transform = 'translateY(12px)';
    });

    animate(statCards,
        { opacity: [0, 1], y: [12, 0] },
        {
            delay: stagger(0.05, { start: 0.2 }),
            easing: spring(springs.snappy),
        }
    );
}

// ── Chart cards fade in on scroll ──
function animateChartCards() {
    const charts = document.querySelectorAll('.chart-card');
    if (!charts.length) return;

    charts.forEach(card => {
        card.style.opacity = '0';
        card.style.transform = 'translateY(16px)';
    });

    inView(charts, (info) => {
        animate(info.target,
            { opacity: [0, 1], y: [16, 0] },
            { duration: 0.5, easing: spring(springs.gentle) }
        );
    }, { margin: '-30px' });
}

// ── Score modal open/close ──
function enhanceScoreModal() {
    const modal = document.getElementById('score-modal');
    if (!modal) return;

    const observer = new MutationObserver((mutations) => {
        for (const m of mutations) {
            if (m.attributeName !== 'class') continue;
            const isOpen = modal.classList.contains('open');
            const inner = modal.querySelector('.score-modal');
            if (!inner) continue;

            if (isOpen) {
                // Backdrop fade
                animate(modal,
                    { opacity: [0, 1] },
                    { duration: 0.2 }
                );
                // Modal spring in
                animate(inner,
                    { opacity: [0, 1], scale: [0.92, 1], y: [20, 0] },
                    { duration: 0.4, easing: spring(springs.bouncy) }
                );
                // Animate score factor bars
                setTimeout(() => {
                    const bars = inner.querySelectorAll('.score-factor-fill');
                    bars.forEach(bar => {
                        const targetWidth = bar.style.width;
                        if (targetWidth && targetWidth !== '0%') {
                            bar.style.width = '0%';
                            animate(bar,
                                { width: ['0%', targetWidth] },
                                { duration: 0.6, delay: 0.1, easing: spring(springs.slow) }
                            );
                        }
                    });
                }, 50);
            }
        }
    });
    observer.observe(modal, { attributes: true });
}

// ── Recalls modal ──
function enhanceRecallsModal() {
    const modal = document.getElementById('recalls-modal');
    if (!modal) return;

    const observer = new MutationObserver((mutations) => {
        for (const m of mutations) {
            if (m.attributeName !== 'class') continue;
            if (modal.classList.contains('open')) {
                const inner = modal.querySelector('.score-modal');
                if (inner) {
                    animate(modal, { opacity: [0, 1] }, { duration: 0.2 });
                    animate(inner,
                        { opacity: [0, 1], scale: [0.92, 1], y: [20, 0] },
                        { duration: 0.4, easing: spring(springs.bouncy) }
                    );
                }
            }
        }
    });
    observer.observe(modal, { attributes: true });
}

// ── Toast notification slide in ──
function enhanceToast() {
    const toast = document.getElementById('scrape-toast');
    if (!toast) return;

    const observer = new MutationObserver(() => {
        if (toast.style.display === 'block') {
            animate(toast,
                { opacity: [0, 1], y: [20, 0], scale: [0.95, 1] },
                { duration: 0.35, easing: spring(springs.snappy) }
            );
        }
    });
    observer.observe(toast, { attributes: true, attributeFilter: ['style'] });
}

// ── Card hover spring effect ──
function addCardHoverEffects() {
    const cards = document.querySelectorAll('.deal-card');
    cards.forEach(card => {
        card.addEventListener('mouseenter', () => {
            animate(card,
                { y: -3, boxShadow: '0 8px 30px rgba(0,0,0,0.3)' },
                { duration: 0.3, easing: spring(springs.snappy) }
            );
        });
        card.addEventListener('mouseleave', () => {
            animate(card,
                { y: 0, boxShadow: '0 0px 0px rgba(0,0,0,0)' },
                { duration: 0.3, easing: spring(springs.snappy) }
            );
        });
    });
}

// ── Badge pop-in animation ──
function animateBadges() {
    const badges = document.querySelectorAll('.deal-badges-primary .badge-score');
    if (!badges.length) return;

    inView(badges, (info) => {
        animate(info.target,
            { scale: [0.5, 1.08, 1], opacity: [0, 1, 1] },
            { duration: 0.4, easing: 'ease-out' }
        );
    }, { margin: '-20px' });
}

// ── Sell page cards ──
function animateSellCards() {
    const cards = document.querySelectorAll('.sell-card, .valuation-card');
    if (!cards.length) return;

    cards.forEach(card => {
        card.style.opacity = '0';
        card.style.transform = 'translateY(16px)';
    });

    animate(cards,
        { opacity: [0, 1], y: [16, 0] },
        {
            delay: stagger(0.08, { start: 0.15 }),
            easing: spring(springs.gentle),
        }
    );
}

// ── Settings sections ──
function animateSettingsSections() {
    const sections = document.querySelectorAll('.settings-section, .setting-card');
    if (!sections.length) return;

    sections.forEach(s => {
        s.style.opacity = '0';
        s.style.transform = 'translateY(12px)';
    });

    animate(sections,
        { opacity: [0, 1], y: [12, 0] },
        {
            delay: stagger(0.06, { start: 0.1 }),
            easing: spring(springs.snappy),
        }
    );
}

// ── Empty state bounce ──
function animateEmptyState() {
    const empty = document.querySelector('.empty-state');
    if (!empty) return;

    animate(empty,
        { opacity: [0, 1], scale: [0.9, 1], y: [30, 0] },
        { duration: 0.5, delay: 0.2, easing: spring(springs.bouncy) }
    );
}

// ── Filter row expand animation ──
function enhanceFilterRow() {
    const filterRow = document.getElementById('filter-row');
    if (!filterRow) return;

    const origToggle = window.toggleMobileFilters;
    if (!origToggle) return;

    window.toggleMobileFilters = function() {
        const wasOpen = filterRow.classList.contains('open');
        origToggle();
        if (!wasOpen) {
            // Just opened
            animate(filterRow,
                { opacity: [0, 1], y: [-8, 0] },
                { duration: 0.25, easing: spring(springs.snappy) }
            );
        }
    };
}

// ── Bottom nav active indicator ──
function animateBottomNav() {
    const active = document.querySelector('.bottom-nav a.active');
    if (!active) {
        const links = document.querySelectorAll('.bottom-nav a');
        links.forEach(link => {
            if (link.classList.contains('active') || window.location.pathname === link.getAttribute('href')) {
                animate(link,
                    { scale: [0.9, 1.05, 1] },
                    { duration: 0.3, easing: 'ease-out' }
                );
            }
        });
    }
}

// ── Health badges (analytics) ──
function animateHealthBadges() {
    const badges = document.querySelectorAll('.health-badge, .health-card');
    if (!badges.length) return;

    inView(badges, (info) => {
        animate(info.target,
            { opacity: [0, 1], scale: [0.9, 1] },
            { duration: 0.35, easing: spring(springs.snappy) }
        );
    });
}

// ── Master init ──
async function initAnimations() {
    const loaded = await initMotion();
    if (!loaded) return;

    // Run immediately — these set initial states
    animatePageEntrance();
    animateDealCards();
    animateStatCards();
    animateChartCards();
    animateSellCards();
    animateSettingsSections();
    animateEmptyState();
    animateBadges();
    animateHealthBadges();
    animateBottomNav();

    // Enhance interactive elements
    addCardHoverEffects();
    enhanceScoreModal();
    enhanceRecallsModal();
    enhanceToast();
    enhanceFilterRow();
}

// Kick off when DOM is ready
if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', initAnimations);
} else {
    initAnimations();
}
