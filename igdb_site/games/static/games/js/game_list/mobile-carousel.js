// games/static/games/js/game_list/mobile-carousel.js

/**
 * Мобильная карусель для списка игр
 * Превращает существующие карточки в горизонтальную карусель на мобильных устройствах
 */

const MobileGameCarousel = {
    initialized: false,
    isMobile: false,
    scrollTimeout: null,

    /**
     * Инициализация карусели
     */
    init: function() {
        if (this.initialized) return;

        this.checkMobile();

        if (!this.isMobile) {
            console.log('MobileGameCarousel: Desktop mode, skipping');
            return;
        }

        console.log('MobileGameCarousel: Initializing');

        if (document.readyState === 'loading') {
            document.addEventListener('DOMContentLoaded', () => this.setupCarousel());
        } else {
            this.setupCarousel();
        }

        // Слушаем обновление сетки игр (после AJAX пагинации)
        document.addEventListener('games-grid-updated', () => {
            setTimeout(() => this.refreshCarousel(), 100);
        });

        window.addEventListener('resize', () => {
            this.checkMobile();
            if (this.isMobile) {
                this.refreshCarousel();
            }
        });

        this.initialized = true;
    },

    /**
     * Проверяет, мобильное ли устройство
     */
    checkMobile: function() {
        this.isMobile = window.innerWidth <= 576;
    },

    /**
     * Настраивает карусель
     */
    setupCarousel: function() {
        const gridRow = document.getElementById('games-grid-row');
        if (!gridRow) return;

        // Если нет карточек, выходим
        const cards = gridRow.querySelectorAll('.game-card-container');
        if (cards.length === 0) return;

        // Добавляем обработчик скролла для обновления индикаторов
        gridRow.addEventListener('scroll', () => {
            clearTimeout(this.scrollTimeout);
            this.scrollTimeout = setTimeout(() => {
                this.updateActiveIndicator(gridRow);
            }, 100);
        });

        // Добавляем кнопки навигации
        this.addNavigationButtons();

        // Обновляем индикаторы
        this.updateIndicators(gridRow);
    },

    /**
     * Добавляет кнопки навигации
     */
    addNavigationButtons: function() {
        const gamesContainer = document.querySelector('.games-container');
        if (!gamesContainer) return;

        // Удаляем старые кнопки
        const oldButtons = gamesContainer.querySelectorAll('.mobile-carousel-btn');
        oldButtons.forEach(btn => btn.remove());

        // Создаем кнопку "влево"
        const leftBtn = document.createElement('button');
        leftBtn.className = 'mobile-carousel-btn left';
        leftBtn.innerHTML = '<i class="bi bi-chevron-left"></i>';
        leftBtn.addEventListener('click', (e) => {
            e.preventDefault();
            this.scrollLeft();
        });

        // Создаем кнопку "вправо"
        const rightBtn = document.createElement('button');
        rightBtn.className = 'mobile-carousel-btn right';
        rightBtn.innerHTML = '<i class="bi bi-chevron-right"></i>';
        rightBtn.addEventListener('click', (e) => {
            e.preventDefault();
            this.scrollRight();
        });

        gamesContainer.appendChild(leftBtn);
        gamesContainer.appendChild(rightBtn);
    },

    /**
     * Прокрутка влево
     */
    scrollLeft: function() {
        const gridRow = document.getElementById('games-grid-row');
        if (!gridRow) return;

        const cardWidth = this.getCardWidth();
        gridRow.scrollBy({
            left: -cardWidth,
            behavior: 'smooth'
        });
    },

    /**
     * Прокрутка вправо
     */
    scrollRight: function() {
        const gridRow = document.getElementById('games-grid-row');
        if (!gridRow) return;

        const cardWidth = this.getCardWidth();
        gridRow.scrollBy({
            left: cardWidth,
            behavior: 'smooth'
        });
    },

    /**
     * Получает ширину карточки с учетом gap
     */
    getCardWidth: function() {
        const gridRow = document.getElementById('games-grid-row');
        if (!gridRow) return 300;

        const firstCard = gridRow.querySelector('.game-card-container');
        if (!firstCard) return 300;

        const styles = window.getComputedStyle(gridRow);
        const gap = parseInt(styles.gap) || 16;

        return firstCard.offsetWidth + gap;
    },

    /**
     * Обновляет индикаторы
     */
    updateIndicators: function(gridRow) {
        const cards = gridRow.querySelectorAll('.game-card-container');
        if (cards.length === 0) return;

        this.updateIndicatorsHTML(cards.length);
        this.updateActiveIndicator(gridRow);
    },

    /**
     * Обновляет HTML индикаторов
     */
    updateIndicatorsHTML: function(totalItems) {
        const gamesContainer = document.querySelector('.games-container');
        if (!gamesContainer) return;

        // Удаляем старые индикаторы
        const oldIndicators = gamesContainer.querySelector('.mobile-carousel-indicators');
        if (oldIndicators) oldIndicators.remove();

        if (totalItems <= 1) return;

        // Создаем новые индикаторы
        const indicators = document.createElement('div');
        indicators.className = 'mobile-carousel-indicators';

        for (let i = 0; i < totalItems; i++) {
            const dot = document.createElement('span');
            dot.className = 'mobile-carousel-dot';
            dot.dataset.index = i;
            dot.addEventListener('click', () => {
                this.scrollToCard(i);
            });
            indicators.appendChild(dot);
        }

        gamesContainer.appendChild(indicators);
    },

    /**
     * Обновляет активный индикатор
     */
    updateActiveIndicator: function(gridRow) {
        const cards = gridRow.querySelectorAll('.game-card-container');
        if (cards.length === 0) return;

        const scrollLeft = gridRow.scrollLeft;
        const cardWidth = this.getCardWidth();

        let currentIndex = Math.round(scrollLeft / cardWidth);
        currentIndex = Math.max(0, Math.min(currentIndex, cards.length - 1));

        // Обновляем активный класс у индикаторов
        const indicators = document.querySelectorAll('.mobile-carousel-dot');
        indicators.forEach((dot, idx) => {
            if (idx === currentIndex) {
                dot.classList.add('active');
            } else {
                dot.classList.remove('active');
            }
        });
    },

    /**
     * Прокручивает к определенной карточке
     */
    scrollToCard: function(index) {
        const gridRow = document.getElementById('games-grid-row');
        if (!gridRow) return;

        const cards = gridRow.querySelectorAll('.game-card-container');
        if (cards.length === 0 || index >= cards.length) return;

        const card = cards[index];
        card.scrollIntoView({
            behavior: 'smooth',
            block: 'nearest',
            inline: 'start'
        });
    },

    /**
     * Обновляет карусель после AJAX загрузки
     */
    refreshCarousel: function() {
        if (!this.isMobile) return;

        const gridRow = document.getElementById('games-grid-row');
        if (!gridRow) return;

        const cards = gridRow.querySelectorAll('.game-card-container');
        if (cards.length === 0) return;

        // Обновляем индикаторы
        this.updateIndicatorsHTML(cards.length);

        // Сбрасываем активный индикатор
        this.updateActiveIndicator(gridRow);

        console.log('MobileGameCarousel: Refreshed with', cards.length, 'cards');
    }
};

// Автоматическая инициализация
document.addEventListener('DOMContentLoaded', () => {
    MobileGameCarousel.init();
});

window.MobileGameCarousel = MobileGameCarousel;