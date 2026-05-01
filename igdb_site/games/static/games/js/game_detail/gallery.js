// games/static/games/js/game_detail/gallery.js
// Оптимизированная галерея с поддержкой карточек и модального окна

( function() {
    'use strict';

    // Состояние галереи
    const state = {
        galleryImages: [],
        cardImages: [],
        currentIndex: 0,
        modalElement: null,
        modalImage: null,
        isCardGallery: false,
        isNavigating: false
    };

    // Конфигурация
    const CONFIG = {
        QUALITY_THRESHOLDS: { width: 800, height: 450 },
        MAX_SCALE: 1.4,        // Уменьшено с 1.8 до 1.4
        SWIPE_THRESHOLD: 50,
        NAVIGATION_COOLDOWN: 300,
        EDGE_ZONE_RATIO: 0.15
    };

    // Кеширование размеров экрана
    let screenSize = { width: window.innerWidth, height: window.innerHeight };

    // Вспомогательные функции
    const debounce = (fn, delay) => {
        let timer;
        return (...args) => {
            clearTimeout(timer);
            timer = setTimeout(() => fn(...args), delay);
        };
    };

    const updateScreenSize = () => {
        screenSize = { width: window.innerWidth, height: window.innerHeight };
    };

    // Масштабирование изображения
    const scaleImage = (img) => {
        if (!img?.src) return;

        const tempImg = new Image();
        tempImg.onload = () => {
            const { width: imgW, height: imgH } = tempImg;
            const { width: screenW, height: screenH } = screenSize;

            const maxWidth = screenW * 0.9;
            const maxHeight = screenH * 0.85;
            let scale = 1;

            if (imgW < CONFIG.QUALITY_THRESHOLDS.width || imgH < CONFIG.QUALITY_THRESHOLDS.height) {
                scale = Math.min(CONFIG.MAX_SCALE, Math.max(
                    CONFIG.QUALITY_THRESHOLDS.width / imgW,
                    CONFIG.QUALITY_THRESHOLDS.height / imgH
                ));
            }

            Object.assign(img.style, {
                transform: scale > 1.05 ? `scale(${scale})` : '',
                transformOrigin: 'center center',
                maxWidth: `${maxWidth}px`,
                maxHeight: `${maxHeight}px`,
                objectFit: 'contain'
            });
        };
        tempImg.src = img.src;
    };

    // Обновление модального окна
    const updateModal = (src, alt, index, total) => {
        if (!state.modalImage) return;

        state.modalImage.src = src;
        state.modalImage.alt = alt || 'Screenshot';

        const currentSpan = document.getElementById('currentNum');
        const totalSpan = document.getElementById('totalCount');

        if (currentSpan) currentSpan.textContent = index + 1;
        if (totalSpan && total) totalSpan.textContent = ` / ${total}`;

        scaleImage(state.modalImage);
    };

    // Навигация с зацикливанием
    const navigate = (direction) => {
        if (state.isNavigating) return;
        state.isNavigating = true;

        const images = state.isCardGallery ? state.cardImages : state.galleryImages;
        if (!images.length) return;

        let newIndex = state.currentIndex;

        if (direction === 'prev') {
            newIndex = state.currentIndex - 1;
            if (newIndex < 0) newIndex = images.length - 1;
        } else {
            newIndex = state.currentIndex + 1;
            if (newIndex >= images.length) newIndex = 0;
        }

        state.currentIndex = newIndex;

        const img = images[state.currentIndex];
        const src = state.isCardGallery ? img.url : (img.getAttribute('data-image') || img.src);
        const alt = state.isCardGallery ? img.alt : (img.alt || 'Screenshot');

        updateModal(src, alt, state.currentIndex, images.length);

        setTimeout(() => {
            state.isNavigating = false;
        }, CONFIG.NAVIGATION_COOLDOWN);
    };

    // Обработчики навигации
    const createNavigationHandlers = () => {
        const prevBtn = document.getElementById('prevBtn');
        const nextBtn = document.getElementById('nextBtn');

        if (prevBtn) prevBtn.onclick = () => navigate('prev');
        if (nextBtn) nextBtn.onclick = () => navigate('next');
    };

    // Обработка свайпов
    const initSwipeHandlers = () => {
        if (!state.modalImage) return;

        let touchStartX = 0;

        state.modalImage.addEventListener('touchstart', (e) => {
            touchStartX = e.touches[0].clientX;
        });

        state.modalImage.addEventListener('touchend', (e) => {
            const diff = e.changedTouches[0].clientX - touchStartX;
            if (Math.abs(diff) >= CONFIG.SWIPE_THRESHOLD) {
                navigate(diff > 0 ? 'prev' : 'next');
            }
        });
    };

    // Обработка кликов по краям
    const initEdgeClickHandlers = () => {
        if (!state.modalElement) return;

        state.modalElement.addEventListener('click', (e) => {
            // Игнорируем клики по элементам управления
            if (e.target.closest('#prevBtn, #nextBtn, .screenshot-close') || state.isNavigating) {
                return;
            }

            const clickX = e.clientX;
            const edgeZone = screenSize.width * CONFIG.EDGE_ZONE_RATIO;

            if (clickX < edgeZone) {
                navigate('prev');
            } else if (clickX > screenSize.width - edgeZone) {
                navigate('next');
            }
        });
    };

    // Открытие галереи из карточек
    window.openGalleryFromCard = (images, startIndex = 0) => {
        if (!state.modalElement) return;

        state.cardImages = images;
        state.isCardGallery = true;
        state.currentIndex = startIndex;

        const img = state.cardImages[state.currentIndex];
        updateModal(img.url, img.alt, state.currentIndex, state.cardImages.length);

        const bsModal = bootstrap.Modal.getInstance(state.modalElement) || new bootstrap.Modal(state.modalElement);
        bsModal.show();
    };

    // Закрытие галереи
    const initModalCloseHandler = () => {
        if (!state.modalElement) return;

        state.modalElement.addEventListener('hidden.bs.modal', () => {
            state.isCardGallery = false;
            state.cardImages = [];
            state.isNavigating = false;
        });
    };

    // Инициализация галереи на странице
    const initInlineGallery = () => {
        state.modalElement = document.getElementById('screenshotModal');
        state.modalImage = document.getElementById('modalImage');

        if (!state.modalElement || !state.modalImage) return;

        // Собираем изображения галереи
        state.galleryImages = Array.from(document.querySelectorAll(
            '.game-carousel-cover, .game-carousel-screenshot, .screenshot-image-fixed, .gallery-img, img[data-image]'
        ));

        if (!state.galleryImages.length) return;

        // Единый обработчик кликов
        if (!window._galleryClickHandler) {
            window._galleryClickHandler = (e) => {
                const img = e.target.closest('.game-carousel-cover, .game-carousel-screenshot, .screenshot-image-fixed, .gallery-img, img[data-image]');
                if (!img || img.closest('#screenshotModal')) return;

                e.preventDefault();
                e.stopPropagation();

                const src = img.getAttribute('data-image') || img.src;
                const index = state.galleryImages.findIndex(i => (i.getAttribute('data-image') || i.src) === src);

                if (index !== -1) {
                    state.currentIndex = index;
                    state.isCardGallery = false;
                    updateModal(src, img.alt, state.currentIndex, state.galleryImages.length);

                    const modal = bootstrap.Modal.getInstance(state.modalElement) || new bootstrap.Modal(state.modalElement);
                    modal.show();
                }
            };
            document.body.addEventListener('click', window._galleryClickHandler);
        }

        // Инициализация всех обработчиков
        createNavigationHandlers();
        initSwipeHandlers();
        initEdgeClickHandlers();
        initModalCloseHandler();

        // Обновление размеров экрана с debounce
        window.addEventListener('resize', debounce(() => {
            updateScreenSize();
            if (state.modalElement?.classList.contains('show') && state.modalImage?.src) {
                scaleImage(state.modalImage);
            }
        }, 150));

        console.log(`✅ Gallery initialized with ${state.galleryImages.length} images`);
    };

    // Запуск инициализации
    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', initInlineGallery);
    } else {
        initInlineGallery();
    }

    console.log('✅ Gallery script ready');
})();