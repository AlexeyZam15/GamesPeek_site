// games/static/games/js/game_detail/header.js
console.log('👤 Header script loaded');

function initializeHeader() {
    document.addEventListener('DOMContentLoaded', function() {
        // Инициализация тултипов для иконок платформ
        var tooltipTriggerList = [].slice.call(document.querySelectorAll('[data-bs-toggle="tooltip"]'));
        var tooltipList = tooltipTriggerList.map(function (tooltipTriggerEl) {
            return new bootstrap.Tooltip(tooltipTriggerEl);
        });

        // Дополнительно инициализируем тултипы для элементов без рейтинга
        setTimeout(function() {
            var noRatingElements = document.querySelectorAll('.no-rating-container');
            noRatingElements.forEach(function(el) {
                if (!el._tooltip) {
                    new bootstrap.Tooltip(el);
                }
            });
        }, 500);
    });
}

// Инициализация при загрузке
initializeHeader();