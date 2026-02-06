// games/static/games/js/game_detail/gallery.js
console.log('🎯 GALLERY SCRIPT LOADED');

let galleryInitialized = false;
let modalInstance = null;
let galleryImages = [];
let currentImageIndex = 0;

// АДАПТИВНЫЕ РАЗМЕРЫ
const FIXED_HEIGHT = 220;
const MIN_WIDTH = 300;
const MAX_SCALE = 1.8;
const ASPECT_RATIO = 16/9;

// Основная функция инициализации галереи
window.initInlineGallery = function() {
    console.log('🚀 initInlineGallery() called');

    // Очищаем предыдущие данные
    galleryImages = [];

    // 1. Находим ВСЕ изображения галереи на странице
    galleryImages = Array.from(document.querySelectorAll('img.gallery-img, img[data-image]'));

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

    // 4. Настраиваем обработчики кликов
    galleryImages.forEach((img, index) => {
        if (!img) return;

        const newImg = img.cloneNode(true);
        img.parentNode.replaceChild(newImg, img);
        img = newImg;

        img.addEventListener('click', handleImageClick);
        img.style.cursor = 'pointer';
        img.style.transition = 'all 0.3s ease';
        img.setAttribute('data-gallery-index', index);
    });

    // 5. Инициализируем модальное окно если еще не инициализировано
    initModal();

    galleryInitialized = true;
    console.log(`✅ Gallery initialized with ${galleryImages.length} images`);
};

// Функция для установки размеров
function setScreenshotSizesNoBorders() {
    console.log(`🎯 Setting screenshot sizes`);

    galleryImages.forEach((img, index) => {
        if (!img) return;

        img.classList.add('screenshot-image-fixed');

        let container = img.closest('.screenshot-container-fixed');
        if (!container) {
            container = img.closest('.screenshot-container');
        }
        if (!container) {
            container = img.closest('.position-relative');
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

    if (originalWidth < MIN_WIDTH) {
        console.log(`📈 Small image detected, enlarging`);

        const scaleFactor = Math.min(MAX_SCALE, MIN_WIDTH / originalWidth);
        const targetWidth = originalWidth * scaleFactor;
        const targetHeight = originalHeight * scaleFactor;

        img.style.width = `${targetWidth}px`;
        img.style.height = `${targetHeight}px`;
        img.style.objectFit = 'scale-down';

        if (originalWidth < 200) {
            const extraScale = 2.2;
            img.style.transform = `scale(${extraScale})`;
            img.style.transformOrigin = 'center center';
        }

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

    img.alt = 'Failed to load screenshot';
    img.innerHTML = '<span>❌ Failed to load</span>';
}

// Обработчик клика по изображению
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

        this.style.width = '';
        this.style.height = '';
        this.style.objectFit = '';
        this.style.backgroundColor = 'transparent';
        this.style.border = 'none';
        this.style.padding = '0';

        if (this.naturalWidth < 1000 || this.naturalHeight < 700) {
            const scaleFactor = Math.min(2.5, 1000 / this.naturalWidth, 700 / this.naturalHeight);
            this.style.width = `${this.naturalWidth * scaleFactor}px`;
            this.style.height = `${this.naturalHeight * scaleFactor}px`;
            this.style.objectFit = 'scale-down';
        } else {
            this.style.maxWidth = '90vw';
            this.style.maxHeight = '85vh';
            this.style.objectFit = 'contain';
        }

        this.style.display = 'block';
        this.style.margin = 'auto';
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