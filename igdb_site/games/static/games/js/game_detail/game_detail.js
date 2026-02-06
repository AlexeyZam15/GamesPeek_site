// games/static/games/js/game_detail/game_detail.js
console.log('🎮 Game detail scripts loaded');

// ===== ФУНКЦИИ ДЛЯ УПРАВЛЕНИЯ ВКЛАДКАМИ =====

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

// Полностью заменяем обработчик Bootstrap
function setupTabListeners() {
    console.log('🔔 Setting up tab listeners');

    const tabElements = document.querySelectorAll('#gameTabs a[data-bs-toggle="tab"]');

    tabElements.forEach(tabElement => {
        tabElement.addEventListener('click', function(event) {
            event.preventDefault();
            event.stopPropagation();

            const targetId = this.getAttribute('href').substring(1);
            console.log(`👆 Switching to tab: ${targetId}`);

            document.querySelectorAll('.tab-pane').forEach(pane => {
                pane.classList.remove('show', 'active');
                pane.style.display = 'none';
            });

            document.querySelectorAll('#gameTabs .nav-link').forEach(link => {
                link.classList.remove('active');
            });

            this.classList.add('active');

            const targetPane = document.getElementById(targetId);
            if (targetPane) {
                targetPane.classList.add('show', 'active');
                targetPane.style.display = 'block';
                targetPane.style.opacity = '1';

                if (targetId === 'screenshots') {
                    setTimeout(() => {
                        if (window.initInlineGallery) {
                            window.initInlineGallery();
                        }
                    }, 100);
                }
            }

            saveActiveTabToUrl(targetId);

            return false;
        });
    });
}

// Функция для активации вкладки по хэшу
function activateTabFromHash() {
    const hash = window.location.hash;

    if (hash && hash.length > 1) {
        const tabId = hash.substring(1);
        const tabLink = document.querySelector(`#gameTabs a[href="#${tabId}"]`);

        if (tabLink) {
            console.log(`🔗 Activating tab from hash: ${tabId}`);
            tabLink.click();
        }
    }
}

// Функция для сохранения активной вкладки в URL
function saveActiveTabToUrl(tabId) {
    if (tabId) {
        console.log(`💾 Saving tab to URL: ${tabId}`);

        if (window.history && window.history.replaceState) {
            const newUrl = window.location.pathname + '#' + tabId;
            window.history.replaceState(null, null, newUrl);
        } else {
            window.location.hash = tabId;
        }

        try {
            localStorage.setItem('game_detail_active_tab', tabId);
        } catch (e) {
            console.log('LocalStorage not available:', e);
        }
    }
}

// ===== ФУНКЦИИ ДЛЯ АДАПТИВНОЙ ВЕРСТКИ =====

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

    const ratingContainer = document.querySelector('.d-flex.justify-content-between.align-items-start');
    if (ratingContainer && window.innerWidth <= 768) {
        ratingContainer.style.flexDirection = 'column';
        ratingContainer.style.alignItems = 'flex-start';
    }
}

// ===== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ДЛЯ ГАЛЕРЕИ =====

// Резервная инициализация галереи
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

// ===== ИНИЦИАЛИЗАЦИЯ =====

function initializeGameDetail() {
    console.log('🏠 DOM fully loaded and parsed');

    // 1. Инициализация Bootstrap tooltips
    var tooltipTriggerList = [].slice.call(document.querySelectorAll('[data-bs-toggle="tooltip"]'));
    var tooltipList = tooltipTriggerList.map(function (tooltipTriggerEl) {
        return new bootstrap.Tooltip(tooltipTriggerEl);
    });

    // 2. Адаптация обложки
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

    // 3. Адаптация мобильного лейаута
    adaptMobileLayout();
    window.addEventListener('resize', adaptMobileLayout);

    // 4. Настройка вкладок
    setTimeout(() => {
        setupTabListeners();

        activateTabFromHash();

        if (!window.location.hash || window.location.hash === '#') {
            try {
                const savedTab = localStorage.getItem('game_detail_active_tab');
                if (savedTab) {
                    const tabLink = document.querySelector(`#gameTabs a[href="#${savedTab}"]`);
                    if (tabLink) {
                        tabLink.click();
                    }
                }
            } catch (e) {
                console.log('LocalStorage not available:', e);
            }
        }

        setTimeout(() => {
            console.log('🔍 Проверка содержимого вкладок:');
            ['overview', 'companies', 'keywords', 'details', 'screenshots'].forEach(tabId => {
                const tab = document.getElementById(tabId);
                if (tab) {
                    console.log(`  ${tabId}:`, {
                        innerHTML: tab.innerHTML.length,
                        children: tab.children.length,
                        display: tab.style.display,
                        classes: tab.className
                    });
                }
            });
        }, 500);

    }, 200);

    // 5. Обработка изменения хэша
    window.addEventListener('hashchange', function() {
        console.log('🔗 Hash changed:', window.location.hash);
        setTimeout(() => {
            activateTabFromHash();
        }, 50);
    });

    // 6. Инициализация галереи
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

// ===== ГЛОБАЛЬНЫЕ ФУНКЦИИ ДЛЯ ОТЛАДКИ =====

window.debugTabs = function() {
    console.log('=== ТАБЫ ОТЛАДКА ===');
    console.log('Хэш:', window.location.hash);

    document.querySelectorAll('#gameTabs a[data-bs-toggle="tab"]').forEach(tab => {
        const tabId = tab.getAttribute('href').substring(1);
        const tabPane = document.getElementById(tabId);

        console.log(`  ${tab.textContent.trim()}:`, {
            href: tab.getAttribute('href'),
            isActive: tab.classList.contains('active'),
            paneExists: !!tabPane,
            paneHTML: tabPane ? tabPane.innerHTML.length : 0,
            paneDisplay: tabPane ? tabPane.style.display : 'none',
            paneClasses: tabPane ? tabPane.className : ''
        });
    });

    console.log('====================');
};

window.forceShowTab = function(tabId) {
    const tabLink = document.querySelector(`#gameTabs a[href="#${tabId}"]`);
    if (tabLink) {
        tabLink.click();
    }
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

    galleryImages.forEach((img, i) => {
        console.log(`  Image ${i}:`, {
            src: img.src.substring(0, 50),
            'data-image': img.getAttribute('data-image'),
            'data-gallery-index': img.getAttribute('data-gallery-index')
        });
    });
};

console.log('✅ Game detail tab management ready');