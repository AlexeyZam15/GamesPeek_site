// games/static/games/js/game_detail/game_detail.js

console.log('🎮 Game detail scripts loaded');

// Функция для переключения сворачиваемого текста
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

// Инициализация десктопной якорной навигации
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

            window.scrollTo({
                top: offsetPosition,
                behavior: 'smooth'
            });

            history.pushState(null, null, targetId);
        }
    }

    anchorLinks.forEach(link => {
        link.addEventListener('click', function(e) {
            e.preventDefault();
            const target = this.getAttribute('href');
            if (target) {
                scrollToAnchor(target);
            }
        });
    });

    window.addEventListener('scroll', function() {
        requestAnimationFrame(updateActiveAnchor);
    });

    updateActiveAnchor();

    if (window.location.hash) {
        setTimeout(() => {
            scrollToAnchor(window.location.hash);
        }, 300);
    }
}

// Инициализация мобильной карусели для якорей
function initMobileAnchorCarousel() {
    const container = document.querySelector('.mobile-anchor-carousel .carousel-container');
    const track = document.getElementById('mobile-carousel-track');
    const cards = document.querySelectorAll('.mobile-anchor-card');
    const dotsContainer = document.getElementById('mobile-carousel-dots');

    if (!container || !track || cards.length === 0) {
        console.log('❌ Mobile carousel elements not found');
        return;
    }

    console.log(`✅ Mobile carousel found with ${cards.length} cards`);

    function scrollToCenter(slideElement) {
        const containerWidth = container.offsetWidth;
        const slideWidth = slideElement.offsetWidth;
        const slideLeft = slideElement.offsetLeft;

        let scrollPosition = slideLeft - (containerWidth / 2) + (slideWidth / 2);
        if (scrollPosition < 0) scrollPosition = 0;

        const maxScroll = track.scrollWidth - containerWidth;
        if (scrollPosition > maxScroll) scrollPosition = maxScroll;

        container.scrollTo({ left: scrollPosition, behavior: 'smooth' });
    }

    function setActiveCard(index) {
        cards.forEach((card, i) => {
            if (i === index) {
                card.classList.add('active');
                card.style.background = '#ff6b35';
                card.style.color = 'white';
            } else {
                card.classList.remove('active');
                card.style.background = 'rgba(255,255,255,0.08)';
                card.style.color = '#e0e0e0';
            }
        });

        const dots = dotsContainer?.querySelectorAll('.carousel-dot');
        dots?.forEach((dot, i) => {
            if (i === index) {
                dot.classList.add('active');
                dot.style.background = '#ff6b35';
                dot.style.width = '20px';
                dot.style.borderRadius = '4px';
            } else {
                dot.classList.remove('active');
                dot.style.background = 'rgba(255,255,255,0.3)';
                dot.style.width = '6px';
                dot.style.borderRadius = '50%';
            }
        });
    }

    function switchToTab(index) {
        if (index < 0 || index >= cards.length) return;

        const card = cards[index];
        const targetId = card.getAttribute('data-target');

        setActiveCard(index);

        const slideElement = card.closest('.carousel-slide');
        if (slideElement) {
            scrollToCenter(slideElement);
        }

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
    }

    cards.forEach((card, index) => {
        const newCard = card.cloneNode(true);
        card.parentNode.replaceChild(newCard, card);
        cards[index] = newCard;

        newCard.addEventListener('click', function(e) {
            e.preventDefault();
            e.stopPropagation();
            console.log(`🔘 Clicked on tab ${index}`);
            switchToTab(index);
        });
    });

    if (dotsContainer) {
        let dotsHtml = '';
        for (let i = 0; i < cards.length; i++) {
            dotsHtml += `<span class="carousel-dot ${i === 0 ? 'active' : ''}" data-index="${i}"></span>`;
        }
        dotsContainer.innerHTML = dotsHtml;

        dotsContainer.querySelectorAll('.carousel-dot').forEach(dot => {
            dot.addEventListener('click', function() {
                const index = parseInt(this.getAttribute('data-index'));
                if (!isNaN(index)) {
                    switchToTab(index);
                }
            });
        });
    }

    container.addEventListener('scroll', function() {
        const scrollLeft = container.scrollLeft;
        const containerWidth = container.offsetWidth;
        let closestIndex = 0;
        let closestDistance = Infinity;

        cards.forEach((card, idx) => {
            const slide = card.closest('.carousel-slide');
            if (slide) {
                const slideCenter = slide.offsetLeft + slide.offsetWidth / 2;
                const viewportCenter = scrollLeft + containerWidth / 2;
                const distance = Math.abs(slideCenter - viewportCenter);
                if (distance < closestDistance) {
                    closestDistance = distance;
                    closestIndex = idx;
                }
            }
        });

        setActiveCard(closestIndex);
    });

    let startIndex = 0;
    const hash = window.location.hash.substring(1);
    const targets = ['game-info', 'overview', 'details', 'companies', 'keywords', 'screenshots'];
    const hashIndex = targets.indexOf(hash);
    if (hashIndex !== -1) startIndex = hashIndex;

    setActiveCard(startIndex);

    setTimeout(() => {
        const activeCard = cards[startIndex];
        if (activeCard) {
            const slideElement = activeCard.closest('.carousel-slide');
            if (slideElement) {
                scrollToCenter(slideElement);
            }
        }
    }, 100);
}

function adjustCoverHeight() {
    const cover = document.querySelector('.game-cover-main');
    if (!cover) return;

    const windowHeight = window.innerHeight;
    const maxAvailableHeight = windowHeight - 200;

    const targetHeight = Math.min(500, maxAvailableHeight);
    cover.style.maxHeight = `${targetHeight}px`;

    const placeholder = document.querySelector('.no-cover-placeholder');
    if (placeholder) {
        placeholder.style.height = `${targetHeight}px`;
    }
}

function adaptMobileLayout() {
    const headerRow = document.querySelector('.row.align-items-start');
    if (headerRow) {
        if (window.innerWidth <= 768) {
            headerRow.classList.add('flex-column');
        } else {
            headerRow.classList.remove('flex-column');
        }
    }
}

function initGalleryFallback() {
    console.log('🔄 Initializing gallery fallback');

    const galleryImages = document.querySelectorAll('img[data-image]');
    console.log(`📸 Found ${galleryImages.length} images with data-image attribute`);

    if (galleryImages.length === 0) return;

    galleryImages.forEach((img, index) => {
        img.style.cursor = 'pointer';
        img.setAttribute('data-gallery-index', index);

        img.addEventListener('click', function(e) {
            e.preventDefault();
            e.stopPropagation();

            const index = parseInt(this.getAttribute('data-gallery-index')) || 0;
            console.log(`🎯 Fallback gallery click on image ${index}`);

            const modalEl = document.getElementById('screenshotModal');
            if (modalEl) {
                const modal = bootstrap.Modal.getInstance(modalEl) || new bootstrap.Modal(modalEl);
                const modalImg = document.getElementById('modalImage');
                const currentNumEl = document.getElementById('currentNum');
                const totalCountEl = document.getElementById('totalCount');

                if (modalImg) {
                    modalImg.src = this.getAttribute('data-image') || this.src;
                    modalImg.alt = this.alt || `Screenshot ${index + 1}`;
                }

                if (currentNumEl) currentNumEl.textContent = index + 1;
                if (totalCountEl) totalCountEl.textContent = ` / ${galleryImages.length}`;

                modalEl.dataset.currentIndex = index;
                modalEl.dataset.totalImages = galleryImages.length;

                modal.show();
                console.log('✅ Fallback modal opened');
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
                const totalImages = parseInt(modalEl.dataset.totalImages) || 0;

                if (currentIndex > 0) {
                    const newIndex = currentIndex - 1;
                    modalEl.dataset.currentIndex = newIndex;

                    const galleryImages = document.querySelectorAll('img[data-image]');
                    const image = galleryImages[newIndex];
                    const modalImg = document.getElementById('modalImage');
                    const currentNumEl = document.getElementById('currentNum');

                    if (modalImg && image) {
                        modalImg.src = image.getAttribute('data-image') || image.src;
                    }

                    if (currentNumEl) {
                        currentNumEl.textContent = newIndex + 1;
                    }
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

                    if (modalImg && image) {
                        modalImg.src = image.getAttribute('data-image') || image.src;
                    }

                    if (currentNumEl) {
                        currentNumEl.textContent = newIndex + 1;
                    }
                }
            });
        }

        modalEl.addEventListener('shown.bs.modal', function() {
            document.addEventListener('keydown', handleKeyboardNavigation);
        });

        modalEl.addEventListener('hidden.bs.modal', function() {
            document.removeEventListener('keydown', handleKeyboardNavigation);
        });
    }

    function handleKeyboardNavigation(e) {
        const modalEl = document.getElementById('screenshotModal');
        if (!modalEl || !modalEl.classList.contains('show')) return;

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
    }

    console.log('✅ Gallery fallback initialized');
}

function initializeGameDetail() {
    console.log('🏠 DOM fully loaded and parsed');

    var tooltipTriggerList = [].slice.call(document.querySelectorAll('[data-bs-toggle="tooltip"]'));
    tooltipTriggerList.map(function (tooltipTriggerEl) {
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
        console.log('🎨 Initializing gallery from main script');
        if (window.initInlineGallery) {
            window.initInlineGallery();
        } else {
            console.error('❌ initInlineGallery function not found');
            initGalleryFallback();
        }
    }, 1000);
}

window.debugTabs = function() {
    console.log('=== ТАБЫ ОТЛАДКА ===');
    console.log('Хэш:', window.location.hash);
    console.log('====================');
};

window.forceInitGallery = function() {
    console.log('🔄 Force initializing gallery');
    if (window.initInlineGallery) {
        window.initInlineGallery();
    } else {
        initGalleryFallback();
    }
};

window.checkGalleryState = function() {
    console.log('🔍 Checking gallery state:');

    const galleryImages = document.querySelectorAll('img[data-image]');
    const modalEl = document.getElementById('screenshotModal');

    console.log(`  Gallery images found: ${galleryImages.length}`);
    console.log(`  Modal exists: ${!!modalEl}`);
    console.log(`  initInlineGallery exists: ${!!window.initInlineGallery}`);
};

document.addEventListener('DOMContentLoaded', function() {
    initializeGameDetail();
});

console.log('✅ Game detail tab management ready');