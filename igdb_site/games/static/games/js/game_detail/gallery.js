// games/static/games/js/game_detail/gallery.js

console.log('🎯 GALLERY SCRIPT LOADED');

let galleryInitialized = false;
let modalInstance = null;
let galleryImages = [];
let currentImageIndex = 0;

// Константы для размеров скриншотов
const MIN_WIDTH = 1280;
const MIN_HEIGHT = 720;
const SUPPORTED_FORMATS = {
    '16:9': { width: 1280, height: 720 },
    '4:3': { width: 960, height: 720 },
    '3:2': { width: 960, height: 640 },
    '21:9': { width: 1680, height: 720 },
    '1:1': { width: 720, height: 720 },
    '9:16': { width: 720, height: 1280 }
};
const MAX_SCALE = 3.0;
const ASPECT_RATIO = 16/9;
const FIXED_HEIGHT = 220;

// Основная функция инициализации галереи
window.initInlineGallery = function() {
    console.log('🚀 initInlineGallery() called');

    // Очищаем предыдущие данные
    galleryImages = [];

    // 1. Находим ВСЕ изображения галереи на странице с расширенным селектором
    galleryImages = Array.from(document.querySelectorAll(
        'img.gallery-img, img[data-image], .screenshot-image-fixed, .screenshot-image, .screenshot-container-fixed img, .screenshot-container img'
    ));

    console.log(`📸 Found ${galleryImages.length} gallery images total on page`);

    if (galleryImages.length === 0) {
        console.warn('⚠️ No gallery images found');
        return;
    }

    // 2. УСТАНАВЛИВАЕМ РАЗМЕРЫ ДЛЯ ВСЕХ СКРИНШОТОВ
    setScreenshotSizesNoBorders();

    // 3. Устанавливаем общее количество в счетчик
    const totalCountEl = document.getElementById('totalCount');
    if (totalCountEl) {
        totalCountEl.textContent = ` / ${galleryImages.length}`;
    }

    // 4. Настраиваем обработчики кликов с использованием делегирования событий
    // Это решает проблему с динамически созданными элементами
    setupClickDelegation();

    // 5. Инициализируем модальное окно если еще не инициализировано
    initModal();

    galleryInitialized = true;
    console.log(`✅ Gallery initialized with ${galleryImages.length} images`);
};

// Новая функция для делегирования событий - более надежный подход
function setupClickDelegation() {
    // Находим контейнер галереи
    const galleryContainer = document.querySelector('.screenshots-gallery, #screenshots');

    if (!galleryContainer) {
        console.warn('⚠️ Gallery container not found, using document delegation');
        // Используем document как fallback
        document.body.addEventListener('click', function(e) {
            const img = e.target.closest('.screenshot-image-fixed, .screenshot-image, .gallery-img, img[data-image]');
            if (img && !img.closest('#screenshotModal')) {
                e.preventDefault();
                e.stopPropagation();
                handleImageClickDelegated(img);
            }
        });
        return;
    }

    // Удаляем старые обработчики если есть
    if (galleryContainer._clickHandler) {
        galleryContainer.removeEventListener('click', galleryContainer._clickHandler);
    }

    // Создаем новый обработчик с делегированием
    const clickHandler = function(e) {
        const img = e.target.closest('.screenshot-image-fixed, .screenshot-image, .gallery-img, img[data-image]');
        if (img && !img.closest('#screenshotModal')) {
            e.preventDefault();
            e.stopPropagation();
            handleImageClickDelegated(img);
        }
    };

    galleryContainer.addEventListener('click', clickHandler);
    galleryContainer._clickHandler = clickHandler;
    console.log('✅ Click delegation set up on gallery container');
}

// Обработчик клика с делегированием
function handleImageClickDelegated(img) {
    // Получаем или устанавливаем индекс
    let index = parseInt(img.getAttribute('data-gallery-index'));

    if (isNaN(index)) {
        // Если индекс не установлен, находим в массиве galleryImages
        const imgSrc = img.getAttribute('data-image') || img.src;
        index = galleryImages.findIndex(item => {
            const itemSrc = item.getAttribute('data-image') || item.src;
            return itemSrc === imgSrc;
        });

        if (index === -1) {
            // Если не нашли, пробуем найти по позиции в DOM
            const allImages = Array.from(document.querySelectorAll(
                '.screenshot-image-fixed, .screenshot-image, .gallery-img, img[data-image]'
            ));
            index = allImages.indexOf(img);
        }

        if (index !== -1) {
            img.setAttribute('data-gallery-index', index);
        }
    }

    console.log(`🎯 Click on image with index ${index}`);

    if (index !== -1 && index < galleryImages.length) {
        openGalleryModal(index);
    } else if (galleryImages.length > 0) {
        // Fallback - открываем первое изображение
        console.warn(`Index ${index} out of range, opening first image`);
        openGalleryModal(0);
    }
}

// Функция для установки размеров
function setScreenshotSizesNoBorders() {
    console.log(`🎯 Setting screenshot sizes`);

    galleryImages.forEach((img, index) => {
        if (!img) return;

        // Убеждаемся что классы установлены
        if (!img.classList.contains('screenshot-image-fixed')) {
            img.classList.add('screenshot-image-fixed');
        }

        // Устанавливаем атрибут индекса
        img.setAttribute('data-gallery-index', index);

        // Убеждаемся что data-image установлен
        if (!img.getAttribute('data-image') && img.src) {
            img.setAttribute('data-image', img.src);
        }

        let container = img.closest('.screenshot-container-fixed');
        if (!container) {
            container = img.closest('.screenshot-container');
        }
        if (!container) {
            container = img.closest('.position-relative');
        }
        if (!container) {
            container = img.closest('.col-md-6, .col-lg-4, .col-xl-3');
        }
        if (!container) {
            container = document.createElement('div');
            container.className = 'screenshot-container-fixed';
            const parent = img.parentNode;
            parent.insertBefore(container, img);
            container.appendChild(img);
        }

        container.style.width = '100%';
        container.style.height = `${FIXED_HEIGHT}px`;
        container.style.overflow = 'hidden';
        container.style.position = 'relative';
        container.style.borderRadius = '8px';
        container.style.display = 'flex';
        container.style.alignItems = 'center';
        container.style.justifyContent = 'center';
        container.style.backgroundColor = 'transparent';

        if (img.complete) {
            calculateAndSetImageSizeNoBorder(img, container);
        } else {
            img.addEventListener('load', function() {
                calculateAndSetImageSizeNoBorder(this, container);
            });

            img.addEventListener('error', function() {
                handleImageErrorNoBorder(this, container);
            });
        }
    });
}

// Расчет и установка размера изображения
function calculateAndSetImageSizeNoBorder(img, container) {
    if (!img || !img.naturalWidth) return;

    const originalWidth = img.naturalWidth;
    const originalHeight = img.naturalHeight;
    const originalRatio = originalWidth / originalHeight;

    console.log(`📐 Image ${originalWidth}x${originalHeight}, ratio: ${originalRatio.toFixed(2)}`);

    img.style.width = '';
    img.style.height = '';
    img.style.transform = '';
    img.style.objectFit = '';
    img.style.backgroundColor = 'transparent';
    img.style.border = 'none';
    img.style.padding = '0';

    // Определяем формат исходного изображения
    const getClosestFormat = (ratio) => {
        const formatRatios = {
            '9:16': 9/16,
            '1:1': 1,
            '4:3': 4/3,
            '3:2': 3/2,
            '16:9': 16/9,
            '21:9': 21/9
        };

        let closestFormat = '16:9';
        let minDiff = Infinity;

        for (const [format, formatRatio] of Object.entries(formatRatios)) {
            const diff = Math.abs(ratio - formatRatio);
            if (diff < minDiff) {
                minDiff = diff;
                closestFormat = format;
            }
        }

        return closestFormat;
    };

    const imageFormat = getClosestFormat(originalRatio);
    const targetSize = SUPPORTED_FORMATS[imageFormat];

    console.log(`🎯 Image format: ${imageFormat}, target size: ${targetSize.width}x${targetSize.height}`);

    if (originalWidth < targetSize.width || originalHeight < targetSize.height) {
        console.log(`📈 Small image detected, scaling up`);

        const widthScale = targetSize.width / originalWidth;
        const heightScale = targetSize.height / originalHeight;
        const scaleFactor = Math.min(MAX_SCALE, Math.max(widthScale, heightScale));

        let targetWidth = originalWidth * scaleFactor;
        let targetHeight = originalHeight * scaleFactor;

        targetWidth = Math.max(targetSize.width, targetWidth);
        targetHeight = Math.max(targetSize.height, targetHeight);

        console.log(`🔍 Scaling from ${originalWidth}x${originalHeight} to ${Math.round(targetWidth)}x${Math.round(targetHeight)} (scale: ${scaleFactor.toFixed(2)}x)`);

        img.style.width = `${targetWidth}px`;
        img.style.height = `${targetHeight}px`;

        if (originalWidth < 200 || originalHeight < 150) {
            const extraScale = Math.min(3, targetSize.width / originalWidth, targetSize.height / originalHeight);
            img.style.transform = `scale(${extraScale})`;
            img.style.transformOrigin = 'center center';
            console.log(`⚡ Extra scale: ${extraScale.toFixed(2)}x`);
        }

        img.style.objectFit = 'scale-down';

    } else {
        if (originalRatio > ASPECT_RATIO) {
            img.style.width = '100%';
            img.style.height = 'auto';
            img.style.objectFit = 'cover';
        } else {
            img.style.width = 'auto';
            img.style.height = '100%';
            img.style.objectFit = 'cover';
        }

        console.log(`✅ Image already meets minimum size for ${imageFormat} format`);
    }

    img.style.display = 'block';
    img.style.margin = 'auto';
}

// Обработка ошибки загрузки изображения
function handleImageErrorNoBorder(img, container) {
    console.error('❌ Failed to load image');

    img.style.width = '100%';
    img.style.height = `${FIXED_HEIGHT}px`;
    img.style.backgroundColor = '#2a2a2a';
    img.style.display = 'flex';
    img.style.alignItems = 'center';
    img.style.justifyContent = 'center';
    img.style.color = '#dc3545';
    img.style.fontSize = '14px';
    img.style.fontWeight = 'bold';
    img.style.borderRadius = '8px';
    img.style.flexDirection = 'column';
    img.style.padding = '20px';

    img.alt = 'Failed to load screenshot';
    img.innerHTML = `
        <span style="margin-bottom: 10px;">❌ Failed to load</span>
        <small style="color: #aaa; font-weight: normal; text-align: center;">
            Min size: 1280×720 (16:9)<br>
            Other formats also supported
        </small>
    `;
}

// Обработчик клика по изображению (оригинальный, оставлен для совместимости)
function handleImageClick(e) {
    e.preventDefault();
    e.stopPropagation();

    const img = this;
    const index = parseInt(img.getAttribute('data-gallery-index')) || 0;

    console.log(`🎯 Click on image ${index}`);
    openGalleryModal(index);
}

// Инициализация модального окна
function initModal() {
    const modalEl = document.getElementById('screenshotModal');

    if (!modalEl) {
        console.error('❌ Modal window element not found');
        return;
    }

    modalInstance = new bootstrap.Modal(modalEl, {
        backdrop: true,
        keyboard: true,
        focus: true
    });

    console.log('✅ Modal instance created');
}

// Открытие модального окна
function openGalleryModal(imageIndex) {
    console.log(`📂 Opening modal for image ${imageIndex}`);

    if (galleryImages.length === 0) {
        console.error('❌ No gallery images available');
        return;
    }

    currentImageIndex = Math.max(0, Math.min(imageIndex, galleryImages.length - 1));
    updateModalContent();

    if (modalInstance) {
        modalInstance.show();
        console.log('✅ Modal window opened');
    }
}

// Обновление содержимого модального окна
function updateModalContent() {
    const currentNumEl = document.getElementById('currentNum');
    const totalCountEl = document.getElementById('totalCount');
    const modalImg = document.getElementById('modalImage');

    if (!modalImg || galleryImages.length === 0) return;

    const currentImage = galleryImages[currentImageIndex];
    if (!currentImage) return;

    const imageUrl = currentImage.getAttribute('data-image') || currentImage.src;

    console.log(`🔄 Loading modal image ${currentImageIndex + 1}/${galleryImages.length}`);

    modalImg.onload = null;
    modalImg.onerror = null;

    modalImg.onload = function() {
        console.log(`✅ Modal image loaded: ${this.naturalWidth}x${this.naturalHeight}`);

        const originalWidth = this.naturalWidth;
        const originalHeight = this.naturalHeight;
        const originalRatio = originalWidth / originalHeight;

        this.style.width = '';
        this.style.height = '';
        this.style.objectFit = '';
        this.style.transform = '';
        this.style.backgroundColor = 'transparent';
        this.style.border = 'none';
        this.style.padding = '0';

        const getClosestFormat = (ratio) => {
            const formatRatios = {
                '9:16': 9/16,
                '1:1': 1,
                '4:3': 4/3,
                '3:2': 3/2,
                '16:9': 16/9,
                '21:9': 21/9
            };

            let closestFormat = '16:9';
            let minDiff = Infinity;

            for (const [format, formatRatio] of Object.entries(formatRatios)) {
                const diff = Math.abs(ratio - formatRatio);
                if (diff < minDiff) {
                    minDiff = diff;
                    closestFormat = format;
                }
            }

            return closestFormat;
        };

        const imageFormat = getClosestFormat(originalRatio);
        const targetSize = SUPPORTED_FORMATS[imageFormat];

        console.log(`🎯 Modal image format: ${imageFormat}, target size: ${targetSize.width}x${targetSize.height}`);

        if (originalWidth < targetSize.width || originalHeight < targetSize.height) {
            const widthScale = targetSize.width / originalWidth;
            const heightScale = targetSize.height / originalHeight;
            const scaleFactor = Math.min(3.0, Math.max(widthScale, heightScale));

            console.log(`📊 Modal scaling from ${originalWidth}x${originalHeight} to ${Math.round(originalWidth * scaleFactor)}x${Math.round(originalHeight * scaleFactor)} (${scaleFactor.toFixed(2)}x)`);

            this.style.width = `${originalWidth}px`;
            this.style.height = `${originalHeight}px`;
            this.style.transform = `scale(${scaleFactor})`;
            this.style.transformOrigin = 'center center';
            this.style.objectFit = 'contain';

        } else {
            this.style.maxWidth = '95vw';
            this.style.maxHeight = '85vh';
            this.style.objectFit = 'contain';
            console.log(`✅ Modal image already meets minimum size`);
        }

        this.style.display = 'block';
        this.style.margin = 'auto';
        this.style.transition = 'transform 0.3s ease';
    };

    modalImg.onerror = function() {
        console.error('❌ Failed to load modal image');
        this.style.backgroundColor = '#2a2a2a';
        this.style.display = 'flex';
        this.style.alignItems = 'center';
        this.style.justifyContent = 'center';
        this.style.width = '500px';
        this.style.height = '300px';
        this.style.color = '#fff';
        this.style.padding = '40px';
        this.style.borderRadius = '8px';
        this.innerHTML = '<div style="text-align: center;"><i class="bi bi-image" style="font-size: 2rem; color: #dc3545;"></i><br><span style="color: #dc3545; font-weight: bold;">Failed to load image</span></div>';
    };

    modalImg.src = imageUrl;
    modalImg.alt = currentImage.alt || `Screenshot ${currentImageIndex + 1}`;

    if (currentNumEl) currentNumEl.textContent = currentImageIndex + 1;
    if (totalCountEl && (!totalCountEl.textContent || totalCountEl.textContent === ' / 0')) {
        totalCountEl.textContent = ` / ${galleryImages.length}`;
    }

    updateNavigationButtons();
}

// Обновление кнопок навигации
function updateNavigationButtons() {
    const prevBtn = document.getElementById('prevBtn');
    const nextBtn = document.getElementById('nextBtn');

    if (prevBtn) {
        prevBtn.disabled = currentImageIndex === 0;
        prevBtn.style.opacity = prevBtn.disabled ? '0.3' : '1';
        prevBtn.style.cursor = prevBtn.disabled ? 'not-allowed' : 'pointer';
        prevBtn.style.pointerEvents = prevBtn.disabled ? 'none' : 'auto';
    }

    if (nextBtn) {
        nextBtn.disabled = currentImageIndex === galleryImages.length - 1;
        nextBtn.style.opacity = nextBtn.disabled ? '0.3' : '1';
        nextBtn.style.cursor = nextBtn.disabled ? 'not-allowed' : 'pointer';
        nextBtn.style.pointerEvents = nextBtn.disabled ? 'none' : 'auto';
    }
}

// Навигация по галерее
function navigateGallery(direction) {
    if (direction === 'prev' && currentImageIndex > 0) {
        currentImageIndex--;
    } else if (direction === 'next' && currentImageIndex < galleryImages.length - 1) {
        currentImageIndex++;
    } else {
        return;
    }

    updateModalContent();
}

// Делаем функцию доступной глобально для вызова извне
window.reinitGallery = function() {
    console.log('🔄 Manual gallery reinitialization');
    window.initInlineGallery();
};

console.log('✅ Gallery script ready');