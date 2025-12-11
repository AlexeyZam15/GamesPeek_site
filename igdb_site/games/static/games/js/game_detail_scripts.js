// game_detail_scripts.js

// Глобальная функция для переключения текста
window.toggleText = function(type) {
    const element = document.getElementById(type + '-text');
    if (!element) return;

    // Получаем кнопку из события
    const button = window.event.currentTarget;
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

// Инициализация при загрузке документа
document.addEventListener('DOMContentLoaded', function () {
    console.log('Game detail scripts loaded');

    // Инициализация Bootstrap tooltips
    var tooltipTriggerList = [].slice.call(document.querySelectorAll('[data-bs-toggle="tooltip"]'));
    var tooltipList = tooltipTriggerList.map(function (tooltipTriggerEl) {
        return new bootstrap.Tooltip(tooltipTriggerEl);
    });

    // Адаптация размера обложки
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

    // Адаптация для мобильных устройств
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

    adaptMobileLayout();
    window.addEventListener('resize', adaptMobileLayout);
});