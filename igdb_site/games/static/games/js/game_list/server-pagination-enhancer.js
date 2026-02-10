// games/static/games/js/game_list/server-pagination-enhancer.js
/**
 * Улучшение UX для серверной пагинации
 * Добавляет анимации и улучшения, которые были в клиентской версии
 */

const ServerPaginationEnhancer = {
    init() {
        console.log('Initializing server pagination enhancer...');

        this.addHoverEffects();
        this.addActivePageAnimation();
        this.addSmoothTransitions();
        this.addLoadingIndicator();
        this.enhancePaginationLinks();

        console.log('Server pagination enhancer initialized');
    },

    addHoverEffects() {
        const paginationLinks = document.querySelectorAll('.games-pagination .page-link');

        paginationLinks.forEach(link => {
            link.addEventListener('mouseenter', function() {
                if (!this.classList.contains('disabled') && !this.parentElement.classList.contains('active')) {
                    this.style.transform = 'translateY(-2px)';
                    this.style.transition = 'all 0.2s ease';
                }
            });

            link.addEventListener('mouseleave', function() {
                if (!this.classList.contains('disabled') && !this.parentElement.classList.contains('active')) {
                    this.style.transform = 'translateY(0)';
                }
            });
        });
    },

    addActivePageAnimation() {
        const activeItems = document.querySelectorAll('.games-pagination .page-item.active');

        activeItems.forEach(item => {
            const link = item.querySelector('.page-link');
            if (link) {
                link.style.animation = 'activePagePulse 2s infinite';
            }
        });
    },

    addSmoothTransitions() {
        document.querySelectorAll('.games-pagination a.page-link').forEach(link => {
            link.addEventListener('click', function(e) {
                const href = this.getAttribute('href');
                if (href && !this.classList.contains('disabled')) {
                    e.preventDefault();

                    // Сохраняем позицию прокрутки через менеджер
                    if (window.ScrollPositionManager && typeof window.ScrollPositionManager.saveScrollPosition === 'function') {
                        window.ScrollPositionManager.saveScrollPosition();
                    }

                    // Показываем индикатор загрузки
                    this.showLoadingIndicator();

                    // Плавная прокрутка к верху
                    window.scrollTo({
                        top: 0,
                        behavior: 'smooth'
                    });

                    // Переход после небольшой задержки
                    setTimeout(() => {
                        window.location.href = href;
                    }, 400);
                }
            });
        });
    },

    addLoadingIndicator() {
        const paginationContainer = document.querySelector('.games-pagination');
        if (paginationContainer) {
            paginationContainer.addEventListener('click', function(e) {
                const target = e.target.closest('a.page-link');
                if (target && !target.classList.contains('disabled')) {
                    target.showLoadingIndicator();
                }
            });
        }
    },

    enhancePaginationLinks() {
        // Добавляем метод showLoadingIndicator ко всем ссылкам пагинации
        HTMLAnchorElement.prototype.showLoadingIndicator = function() {
            const originalHTML = this.innerHTML;
            const originalWidth = this.offsetWidth;

            // Сохраняем оригинальный контент и ширину
            this.setAttribute('data-original-html', originalHTML);
            this.setAttribute('data-original-width', originalWidth);

            // Устанавливаем индикатор загрузки
            this.innerHTML = `
                <span class="spinner-border spinner-border-sm me-2" role="status" aria-hidden="true"></span>
                Loading...
            `;
            this.style.minWidth = originalWidth + 'px';
            this.classList.add('loading');
            this.style.pointerEvents = 'none';

            // Возвращаем оригинальный вид через 5 секунд (на случай если что-то пошло не так)
            setTimeout(() => {
                this.resetLoadingIndicator();
            }, 5000);
        };

        // Метод для сброса индикатора
        HTMLAnchorElement.prototype.resetLoadingIndicator = function() {
            const originalHTML = this.getAttribute('data-original-html');
            if (originalHTML) {
                this.innerHTML = originalHTML;
                this.removeAttribute('data-original-html');
                this.removeAttribute('data-original-width');
                this.style.minWidth = '';
                this.classList.remove('loading');
                this.style.pointerEvents = '';
            }
        };

        // Сбрасываем индикаторы при загрузке страницы
        document.addEventListener('DOMContentLoaded', () => {
            document.querySelectorAll('.page-link.loading').forEach(link => {
                link.resetLoadingIndicator();
            });
        });
    }
};

// Инициализация при загрузке DOM
document.addEventListener('DOMContentLoaded', function() {
    ServerPaginationEnhancer.init();
});

// Экспорт для использования в других модулях
if (typeof module !== 'undefined' && module.exports) {
    module.exports = ServerPaginationEnhancer;
} else {
    window.ServerPaginationEnhancer = ServerPaginationEnhancer;
}