// games/static/games/js/game_detail/game_detail.js

console.log('🎮 Game detail scripts loaded');

window.toggleText = function(type) {
    const element = document.getElementById(type + '-text');
    if (!element) return;

    const button = window.event?.currentTarget;
    if (button) {
        const icon = button.querySelector('i');
        const span = button.querySelector('span');

        if (element.classList.contains('expanded')) {
            element.classList.remove('expanded');
            if (span) span.textContent = 'Read more';
            if (icon) icon.className = 'bi bi-chevron-down';
        } else {
            element.classList.add('expanded');
            if (span) span.textContent = 'Show less';
            if (icon) icon.className = 'bi bi-chevron-up';
        }
    }
};

function initAnchorNavigation() {
    const anchorLinks = document.querySelectorAll('.global-anchor-link');
    const headerOffset = 70;
    const sections = ['game-info', 'overview', 'details', 'companies', 'keywords', 'screenshots'];
    const progressBar = document.getElementById('anchor-progress-bar');

    function updateActiveAnchor() {
        const scrollPosition = window.scrollY + headerOffset + 50;

        for (let i = sections.length - 1; i >= 0; i--) {
            const section = document.getElementById(sections[i]);
            if (section) {
                const sectionTop = section.offsetTop;
                if (scrollPosition >= sectionTop) {
                    document.querySelectorAll('.global-anchor-link').forEach(link => {
                        link.classList.remove('active');
                        if (link.getAttribute('data-anchor') === sections[i]) {
                            link.classList.add('active');
                        }
                    });
                    break;
                }
            }
        }

        if (progressBar) {
            const scrollPercent = (window.scrollY / (document.documentElement.scrollHeight - window.innerHeight)) * 100;
            progressBar.style.width = scrollPercent + '%';
        }
    }

    function scrollToAnchor(targetId) {
        const targetElement = document.querySelector(targetId);
        if (targetElement) {
            const elementPosition = targetElement.getBoundingClientRect().top;
            const offsetPosition = elementPosition + window.pageYOffset - headerOffset;
            window.scrollTo({ top: offsetPosition, behavior: 'smooth' });
            history.pushState(null, null, targetId);
        }
    }

    anchorLinks.forEach(link => {
        link.addEventListener('click', function(e) {
            e.preventDefault();
            const target = this.getAttribute('href');
            if (target) scrollToAnchor(target);
        });
    });

    window.addEventListener('scroll', function() {
        requestAnimationFrame(updateActiveAnchor);
    });

    updateActiveAnchor();

    if (window.location.hash) {
        setTimeout(() => scrollToAnchor(window.location.hash), 300);
    }
}

function initMobileAnchorCarousel() {
    const carousel = document.querySelector('.mobile-anchor-carousel');
    if (!carousel) return;

    const container = carousel.querySelector('.carousel-container');
    const dotsContainer = document.getElementById('mobile-carousel-dots');

    const newTrack = document.createElement('div');
    newTrack.className = 'carousel-track';
    newTrack.id = 'mobile-carousel-track';
    newTrack.style.cssText = 'display: flex; gap: 8px; padding: 0 12px;';

    // Обновленный массив якорей с добавлением similar-games
    const anchors = ['game-info', 'overview', 'details', 'companies', 'keywords', 'screenshots', 'similar-games'];
    const icons = ['bi-controller', 'bi-info-circle', 'bi-list-check', 'bi-building', 'bi-tags', 'bi-image', 'bi-search-heart'];
    const labels = ['Game Info', 'Overview', 'Details', 'Companies', 'Keywords', 'Screenshots', 'Similar Games'];

    anchors.forEach((anchor, i) => {
        // Проверяем, существует ли элемент с таким id на странице
        const targetElement = document.getElementById(anchor);
        if (!targetElement && anchor === 'similar-games') {
            return; // Пропускаем similar-games если блока нет на странице
        }

        const slide = document.createElement('div');
        slide.className = 'carousel-slide';
        slide.setAttribute('data-anchor', anchor);
        slide.style.cssText = 'flex: 0 0 auto; scroll-snap-align: start;';

        const card = document.createElement('div');
        card.className = 'mobile-anchor-card';
        card.setAttribute('data-target', '#' + anchor);
        card.style.cssText = 'display: flex; align-items: center; gap: 8px; padding: 8px 16px; border-radius: 30px; font-size: 0.85rem; font-weight: 500; white-space: nowrap; cursor: pointer; transition: all 0.2s ease; background: rgba(255,255,255,0.08); color: #e0e0e0;';

        const icon = document.createElement('i');
        icon.className = icons[i];

        const span = document.createElement('span');
        span.textContent = labels[i];

        card.appendChild(icon);
        card.appendChild(span);
        slide.appendChild(card);
        newTrack.appendChild(slide);
    });

    const oldTrack = document.getElementById('mobile-carousel-track');
    if (oldTrack && oldTrack.parentNode) {
        oldTrack.parentNode.replaceChild(newTrack, oldTrack);
    } else if (container) {
        container.appendChild(newTrack);
    }

    const cards = document.querySelectorAll('.mobile-anchor-card');

    function scrollCarouselToSlide(slideElement) {
        if (!container || !slideElement) return;
        const containerWidth = container.offsetWidth;
        const slideWidth = slideElement.offsetWidth;
        const slideLeft = slideElement.offsetLeft;

        let scrollPosition = slideLeft - (containerWidth / 2) + (slideWidth / 2);
        if (scrollPosition < 0) scrollPosition = 0;

        const maxScroll = newTrack.scrollWidth - containerWidth;
        if (scrollPosition > maxScroll) scrollPosition = maxScroll;

        container.scrollTo({ left: scrollPosition, behavior: 'smooth' });
    }

    function setActive(index) {
        cards.forEach((card, i) => {
            if (i === index) {
                card.style.background = '#ff6b35';
                card.style.color = 'white';
                card.classList.add('active');
            } else {
                card.style.background = 'rgba(255,255,255,0.08)';
                card.style.color = '#e0e0e0';
                card.classList.remove('active');
            }
        });

        const dots = document.querySelectorAll('.carousel-dot');
        dots.forEach((dot, i) => {
            if (i === index) {
                dot.style.background = '#ff6b35';
                dot.style.width = '20px';
                dot.style.borderRadius = '4px';
            } else {
                dot.style.background = 'rgba(255,255,255,0.3)';
                dot.style.width = '6px';
                dot.style.borderRadius = '50%';
            }
        });

        const slideToScroll = cards[index].closest('.carousel-slide');
        if (slideToScroll) {
            scrollCarouselToSlide(slideToScroll);
        }
    }

    cards.forEach((card, index) => {
        card.addEventListener('click', function(e) {
            e.preventDefault();
            e.stopPropagation();
            setActive(index);

            const targetId = this.getAttribute('data-target');
            if (targetId) {
                const targetElement = document.querySelector(targetId);
                if (targetElement) {
                    const headerOffset = 110;
                    const elementPosition = targetElement.getBoundingClientRect().top;
                    const offsetPosition = elementPosition + window.pageYOffset - headerOffset;
                    window.scrollTo({ top: offsetPosition, behavior: 'smooth' });
                    history.pushState(null, null, targetId);
                }
            }
        });
    });

    if (dotsContainer) {
        dotsContainer.innerHTML = '';
        for (let i = 0; i < cards.length; i++) {
            const dot = document.createElement('span');
            dot.className = 'carousel-dot';
            dot.style.cssText = 'width: 6px; height: 6px; border-radius: 50%; background: rgba(255,255,255,0.3); transition: all 0.2s ease; cursor: pointer;';
            dot.setAttribute('data-index', i);
            dot.addEventListener('click', function() {
                const idx = parseInt(this.getAttribute('data-index'));
                setActive(idx);
                const targetId = cards[idx].getAttribute('data-target');
                if (targetId) {
                    const targetElement = document.querySelector(targetId);
                    if (targetElement) {
                        const headerOffset = 110;
                        const elementPosition = targetElement.getBoundingClientRect().top;
                        const offsetPosition = elementPosition + window.pageYOffset - headerOffset;
                        window.scrollTo({ top: offsetPosition, behavior: 'smooth' });
                        history.pushState(null, null, targetId);
                    }
                }
            });
            dotsContainer.appendChild(dot);
        }
    }

    let startIndex = 0;
    const hash = window.location.hash.substring(1);
    const idx = anchors.indexOf(hash);
    if (idx !== -1 && idx < cards.length) startIndex = idx;
    if (cards.length > 0) setActive(startIndex);
}

function adjustCoverHeight() {
    const cover = document.querySelector('.game-cover-main');
    if (!cover) return;
    const windowHeight = window.innerHeight;
    const targetHeight = Math.min(500, windowHeight - 200);
    cover.style.maxHeight = `${targetHeight}px`;
    const placeholder = document.querySelector('.no-cover-placeholder');
    if (placeholder) placeholder.style.height = `${targetHeight}px`;
}

function adaptMobileLayout() {
    const headerRow = document.querySelector('.row.align-items-start');
    if (headerRow) {
        if (window.innerWidth <= 768) headerRow.classList.add('flex-column');
        else headerRow.classList.remove('flex-column');
    }
}

function initGalleryFallback() {
    const galleryImages = document.querySelectorAll('img[data-image]');
    if (galleryImages.length === 0) return;

    galleryImages.forEach((img, index) => {
        img.style.cursor = 'pointer';
        img.setAttribute('data-gallery-index', index);
        img.addEventListener('click', function(e) {
            e.preventDefault();
            e.stopPropagation();
            const idx = parseInt(this.getAttribute('data-gallery-index')) || 0;
            const modalEl = document.getElementById('screenshotModal');
            if (modalEl) {
                const modal = bootstrap.Modal.getInstance(modalEl) || new bootstrap.Modal(modalEl);
                const modalImg = document.getElementById('modalImage');
                const currentNumEl = document.getElementById('currentNum');
                const totalCountEl = document.getElementById('totalCount');
                if (modalImg) modalImg.src = this.getAttribute('data-image') || this.src;
                if (currentNumEl) currentNumEl.textContent = idx + 1;
                if (totalCountEl) totalCountEl.textContent = ` / ${galleryImages.length}`;
                modalEl.dataset.currentIndex = idx;
                modalEl.dataset.totalImages = galleryImages.length;
                modal.show();
            }
        });
    });

    const modalEl = document.getElementById('screenshotModal');
    if (modalEl) {
        const prevBtn = modalEl.querySelector('#prevBtn');
        const nextBtn = modalEl.querySelector('#nextBtn');

        if (prevBtn) {
            prevBtn.addEventListener('click', function(e) {
                e.preventDefault();
                const currentIndex = parseInt(modalEl.dataset.currentIndex) || 0;
                if (currentIndex > 0) {
                    const newIndex = currentIndex - 1;
                    modalEl.dataset.currentIndex = newIndex;
                    const galleryImages = document.querySelectorAll('img[data-image]');
                    const image = galleryImages[newIndex];
                    const modalImg = document.getElementById('modalImage');
                    const currentNumEl = document.getElementById('currentNum');
                    if (modalImg && image) modalImg.src = image.getAttribute('data-image') || image.src;
                    if (currentNumEl) currentNumEl.textContent = newIndex + 1;
                }
            });
        }

        if (nextBtn) {
            nextBtn.addEventListener('click', function(e) {
                e.preventDefault();
                const currentIndex = parseInt(modalEl.dataset.currentIndex) || 0;
                const totalImages = parseInt(modalEl.dataset.totalImages) || 0;
                if (currentIndex < totalImages - 1) {
                    const newIndex = currentIndex + 1;
                    modalEl.dataset.currentIndex = newIndex;
                    const galleryImages = document.querySelectorAll('img[data-image]');
                    const image = galleryImages[newIndex];
                    const modalImg = document.getElementById('modalImage');
                    const currentNumEl = document.getElementById('currentNum');
                    if (modalImg && image) modalImg.src = image.getAttribute('data-image') || image.src;
                    if (currentNumEl) currentNumEl.textContent = newIndex + 1;
                }
            });
        }

        modalEl.addEventListener('shown.bs.modal', function() {
            document.addEventListener('keydown', function(e) {
                if (!modalEl.classList.contains('show')) return;
                const currentIndex = parseInt(modalEl.dataset.currentIndex) || 0;
                const totalImages = parseInt(modalEl.dataset.totalImages) || 0;
                if (e.key === 'ArrowLeft' && currentIndex > 0) {
                    e.preventDefault();
                    modalEl.querySelector('#prevBtn').click();
                } else if (e.key === 'ArrowRight' && currentIndex < totalImages - 1) {
                    e.preventDefault();
                    modalEl.querySelector('#nextBtn').click();
                } else if (e.key === 'Escape') {
                    const modal = bootstrap.Modal.getInstance(modalEl);
                    if (modal) modal.hide();
                }
            });
        });
    }
}

function initializeGameDetail() {
    var tooltipTriggerList = [].slice.call(document.querySelectorAll('[data-bs-toggle="tooltip"]'));
    tooltipTriggerList.map(function(tooltipTriggerEl) {
        return new bootstrap.Tooltip(tooltipTriggerEl);
    });

    adjustCoverHeight();
    window.addEventListener('resize', adjustCoverHeight);

    const coverImg = document.querySelector('.game-cover-main');
    if (coverImg) {
        coverImg.onload = function() {
            adjustCoverHeight();
            this.style.opacity = '1';
        };
        if (coverImg.complete) {
            adjustCoverHeight();
            coverImg.style.opacity = '1';
        }
    }

    adaptMobileLayout();
    window.addEventListener('resize', adaptMobileLayout);

    initAnchorNavigation();
    initMobileAnchorCarousel();

    setTimeout(() => {
        if (window.initInlineGallery) {
            window.initInlineGallery();
        } else {
            initGalleryFallback();
        }
    }, 1000);
}

window.forceInitGallery = function() {
    if (window.initInlineGallery) {
        window.initInlineGallery();
    } else {
        initGalleryFallback();
    }
};

document.addEventListener('DOMContentLoaded', function() {
    initializeGameDetail();
});

console.log('✅ Game detail ready');