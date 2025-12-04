// Инициализация Fancybox
Fancybox.bind("[data-fancybox]", {
    Thumbs: {
        type: "classic",
    },
    Toolbar: {
        display: {
            left: [],
            middle: [],
            right: ["close"],
        },
    },
    Images: {
        zoom: false,
    },
});

function toggleText(type) {
    const element = document.getElementById(`${type}-text`);
    const button = event.currentTarget.closest('.toggle-text-btn'); // Исправлено: currentTarget вместо target
    const icon = button.querySelector('i');
    const span = button.querySelector('span');

    if (element.classList.contains('expanded')) {
        element.classList.remove('expanded');
        span.textContent = 'Read more';
        icon.className = 'bi bi-chevron-down';
    } else {
        element.classList.add('expanded');
        span.textContent = 'Show less';
        icon.className = 'bi bi-chevron-up';
    }
}

// Инициализация tooltips
document.addEventListener('DOMContentLoaded', function () {
    var tooltipTriggerList = [].slice.call(document.querySelectorAll('[data-bs-toggle="tooltip"]'));
    var tooltipList = tooltipTriggerList.map(function (tooltipTriggerEl) {
        return new bootstrap.Tooltip(tooltipTriggerEl);
    });

    // Адаптация размера обложки
    function adjustCoverHeight() {
        const cover = document.querySelector('.game-cover-main');
        if (!cover) return;

        const windowHeight = window.innerHeight;
        const maxAvailableHeight = windowHeight - 200; // Минус отступы

        // Устанавливаем высоту обложки в зависимости от высоты окна
        const targetHeight = Math.min(500, maxAvailableHeight);

        cover.style.maxHeight = `${targetHeight}px`;

        // Адаптируем placeholder
        const placeholder = document.querySelector('.no-cover-placeholder');
        if (placeholder) {
            placeholder.style.height = `${targetHeight}px`;
        }
    }

    // Вызываем при загрузке и изменении размера окна
    adjustCoverHeight();
    window.addEventListener('resize', adjustCoverHeight);

    // Также вызываем после полной загрузки изображения
    const coverImg = document.querySelector('.game-cover-main');
    if (coverImg) {
        coverImg.onload = function() {
            adjustCoverHeight();
            this.style.opacity = '1';
        };

        // Если изображение уже загружено (из кэша)
        if (coverImg.complete) {
            adjustCoverHeight();
            coverImg.style.opacity = '1';
        }
    }

    // Адаптация для мобильных устройств - динамическая
    function adaptMobileLayout() {
        const headerRow = document.querySelector('.row.align-items-start');
        if (headerRow) {
            if (window.innerWidth <= 768) {
                headerRow.classList.add('flex-column');
            } else {
                headerRow.classList.remove('flex-column');
            }
        }

        // Адаптация для рейтинга на мобильных
        const ratingContainer = document.querySelector('.d-flex.justify-content-between.align-items-start');
        if (ratingContainer && window.innerWidth <= 768) {
            ratingContainer.style.flexDirection = 'column';
            ratingContainer.style.alignItems = 'flex-start';
        }
    }

    // Вызываем адаптацию при загрузке и изменении размера
    adaptMobileLayout();
    window.addEventListener('resize', adaptMobileLayout);

    // Добавляем обработчики для кнопок "Read more"
    document.querySelectorAll('.toggle-text-btn').forEach(button => {
        button.addEventListener('click', function(e) {
            // Находим ближайший текстовый элемент
            const textElement = this.closest('.game-detail-card').querySelector('.collapsible-text');
            if (textElement) {
                const type = textElement.id.replace('-text', '');
                const icon = this.querySelector('i');
                const span = this.querySelector('span');

                if (textElement.classList.contains('expanded')) {
                    textElement.classList.remove('expanded');
                    span.textContent = 'Read more';
                    icon.className = 'bi bi-chevron-down';
                } else {
                    textElement.classList.add('expanded');
                    span.textContent = 'Show less';
                    icon.className = 'bi bi-chevron-up';
                }
            }
        });
    });
});