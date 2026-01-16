// games/static/games/js/modules/filters-sticky.js

const FilterSticky = {
    // Элементы
    filtersContainer: null,
    originalButtonsContainer: null,
    fixedButtonsContainer: null,

    // Состояние
    isFiltersVisible: false,

    // Конфигурация
    config: {
        offset: 100,
        checkInterval: 100,
        mobileBreakpoint: 768
    },

    // Флаг чтобы не создавать дубликаты
    initialized: false,

    // Инициализация
    init() {
        // Проверяем, не инициализирован ли уже
        if (this.initialized) {
            return;
        }

        // Находим элементы
        this.filtersContainer = document.querySelector('.card.mb-4');
        this.originalButtonsContainer = document.querySelector('.apply-filters-main')?.closest('.d-flex.gap-2.flex-wrap');

        if (!this.filtersContainer || !this.originalButtonsContainer) {
            return;
        }

        // Удаляем старый фиксированный контейнер если он есть
        this.removeExistingFixedContainer();

        // Создаем фиксированный контейнер для кнопок
        this.createFixedButtons();

        // Сразу устанавливаем позицию снизу слева
        this.setBottomLeftPosition();

        // Настраиваем отслеживание видимости
        this.setupVisibilityTracking();

        // Настраиваем обработчики
        this.setupEventListeners();

        // Настраиваем адаптивность
        this.setupResponsive();

        // Первоначальная проверка
        this.checkVisibility();

        // Помечаем как инициализированный
        this.initialized = true;
    },

    // Удаляем существующий фиксированный контейнер
    removeExistingFixedContainer() {
        const existingContainers = document.querySelectorAll('.apply-filters-fixed-container');
        existingContainers.forEach(container => {
            container.remove();
        });
    },

    // Создаем фиксированный контейнер для кнопок
    createFixedButtons() {
        // Проверяем, нет ли уже контейнера
        if (document.querySelector('.apply-filters-fixed-container')) {
            this.fixedButtonsContainer = document.querySelector('.apply-filters-fixed-container');
            return;
        }

        // Клонируем кнопки из основного контейнера
        const buttonsClone = this.originalButtonsContainer.cloneNode(true);
        buttonsClone.className = 'apply-filters-fixed-container';

        // Сразу скрываем контейнер
        buttonsClone.classList.add('hidden');

        // Добавляем контейнер в body
        document.body.appendChild(buttonsClone);
        this.fixedButtonsContainer = buttonsClone;
    },

    // Устанавливаем позицию снизу слева
    setBottomLeftPosition() {
        if (!this.fixedButtonsContainer) return;

        // Всегда снизу слева
        this.fixedButtonsContainer.style.bottom = '30px';
        this.fixedButtonsContainer.style.left = '30px';
        this.fixedButtonsContainer.style.right = 'auto';
        this.fixedButtonsContainer.style.top = 'auto';
        this.fixedButtonsContainer.style.transform = 'none';
    },

    // Настройка отслеживания видимости
    setupVisibilityTracking() {
        // Отслеживаем скролл для определения видимости
        const checkVisibilityThrottled = this.throttle(() => this.checkVisibility(), 50);
        window.addEventListener('scroll', checkVisibilityThrottled);
        window.addEventListener('resize', checkVisibilityThrottled);

        // Периодическая проверка
        setInterval(() => this.checkVisibility(), this.config.checkInterval);
    },

    // throttle функция для оптимизации
    throttle(func, limit) {
        let inThrottle;
        return function() {
            const args = arguments;
            const context = this;
            if (!inThrottle) {
                func.apply(context, args);
                inThrottle = true;
                setTimeout(() => inThrottle = false, limit);
            }
        };
    },

    // Проверка видимости элементов
    checkVisibility() {
        if (!this.filtersContainer || !this.fixedButtonsContainer) return;

        // Проверяем видимость фильтров
        const filtersRect = this.filtersContainer.getBoundingClientRect();
        const windowHeight = window.innerHeight;

        this.isFiltersVisible = (
            filtersRect.top < windowHeight - this.config.offset &&
            filtersRect.bottom > this.config.offset
        );

        // Обновляем состояние фиксированных кнопок
        this.updateFixedButtonsState();
    },

    // Обновление состояния фиксированных кнопок
    updateFixedButtonsState() {
        if (!this.fixedButtonsContainer) return;

        // Фиксированные кнопки показываем, если фильтры видны
        if (this.isFiltersVisible) {
            this.showFixedButtons();
        } else {
            this.hideFixedButtons();
        }

        // Убрали пульсацию при выбранных фильтрах
        const hasSelections = this.getSelectedFiltersCount() > 0;
        if (hasSelections) {
            // Ничего не делаем, убрали бейдж и пульсацию
        } else {
            // Ничего не делаем
        }
    },

    // Показать фиксированные кнопки
    showFixedButtons() {
        if (!this.fixedButtonsContainer.classList.contains('visible')) {
            this.fixedButtonsContainer.classList.remove('hidden');
            this.fixedButtonsContainer.classList.add('visible');
        }
    },

    // Скрыть фиксированные кнопки
    hideFixedButtons() {
        if (this.fixedButtonsContainer.classList.contains('visible')) {
            this.fixedButtonsContainer.classList.remove('visible');
            this.fixedButtonsContainer.classList.add('hidden');
        }
    },

    // Настройка обработчиков событий
    setupEventListeners() {
        if (!this.fixedButtonsContainer) return;

        // Находим кнопки в фиксированном контейнере
        const applyButton = this.fixedButtonsContainer.querySelector('.apply-filters-main');
        const showAllButton = this.fixedButtonsContainer.querySelector('a[href*="game_list"]');

        // Обработчик для Apply Filters
        if (applyButton) {
            // Удаляем старые обработчики
            const newApplyButton = applyButton.cloneNode(true);
            applyButton.parentNode.replaceChild(newApplyButton, applyButton);

            // Добавляем новый обработчик
            newApplyButton.addEventListener('click', (e) => {
                e.preventDefault();
                e.stopPropagation();

                // Сохраняем позицию прокрутки
                this.saveScrollPosition();

                // Отправляем форму
                const form = document.getElementById('main-search-form');
                if (form) {
                    setTimeout(() => form.submit(), 100);
                }
            });
        }

        // Обработчик для Show All Games
        if (showAllButton) {
            // Удаляем старые обработчики
            const newShowAllButton = showAllButton.cloneNode(true);
            showAllButton.parentNode.replaceChild(newShowAllButton, showAllButton);

            // Добавляем новый обработчик
            newShowAllButton.addEventListener('click', (e) => {
                e.preventDefault();
                e.stopPropagation();

                // Сохраняем позицию прокрутки
                this.saveScrollPosition();

                // Переходим по ссылке
                setTimeout(() => {
                    window.location.href = showAllButton.getAttribute('href');
                }, 100);
            });
        }

        // Следим за изменениями в фильтрах (без бейджа)
        this.setupFilterChangeListeners();
    },

    // Настройка слушателей изменений фильтров
    setupFilterChangeListeners() {
        // Следим за чекбоксами (без обновления бейджа)
        document.addEventListener('change', (e) => {
            if (e.target.matches('input[type="checkbox"]')) {
                // Убрали обновление бейджа
            }
        });
    },

    // Получение количества выбранных фильтров (оставляем для возможного использования)
    getSelectedFiltersCount() {
        const checkboxes = document.querySelectorAll('input[type="checkbox"]:checked');
        return checkboxes.length;
    },

    // Обновление бейджа с количеством фильтров - УДАЛЕНО

    // Настройка адаптивности
    setupResponsive() {
        window.addEventListener('resize', () => {
            // Переустанавливаем позицию при ресайзе
            this.setBottomLeftPosition();

            // Перепроверяем видимость
            this.checkVisibility();
        });
    },

    // Сохранение позиции прокрутки
    saveScrollPosition() {
        try {
            const scrollY = window.scrollY || document.documentElement.scrollTop;
            if (scrollY > 100) {
                sessionStorage.setItem('filterScrollY', scrollY.toString());
            }
        } catch (e) {
            // Игнорируем ошибки
        }
    },

    // Очистка при уничтожении
    destroy() {
        if (this.fixedButtonsContainer) {
            this.fixedButtonsContainer.remove();
        }
        this.initialized = false;
    }
};

// Автоматическая инициализация с защитой от дублирования
document.addEventListener('DOMContentLoaded', () => {
    // Удаляем любые существующие фиксированные контейнеры
    const existingContainers = document.querySelectorAll('.apply-filters-fixed-container');
    existingContainers.forEach(container => container.remove());

    // Инициализируем с задержкой
    setTimeout(() => {
        FilterSticky.init();
    }, 800);
});

// Также инициализируем при полной загрузке страницы
window.addEventListener('load', () => {
    setTimeout(() => {
        if (!FilterSticky.initialized) {
            FilterSticky.init();
        }
    }, 500);
});

export default FilterSticky;