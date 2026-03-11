// games/static/games/js/game_list/filters.js

// Добавляем служебный объект для таймеров
const FilterManagerDebugTimer = {
    marks: {},
    start(label) {
        this.marks[label] = performance.now();
    },
    end(label) {
        const endTime = performance.now();
        const startTime = this.marks[label];
        if (startTime) {
            const duration = (endTime - startTime).toFixed(2);
            console.warn(`[TIMER] ${label} took ${duration} ms`);
            delete this.marks[label];
        } else {
            console.warn(`[TIMER] No start mark found for: ${label}`);
        }
    }
};

// Главный файл-загрузчик для модулей фильтров
// Реэкспортирует все модули и предоставляет точку входа

// Импортируем все модули
import FilterGlobal from './filters_js/filters-global.js';
import FilterHandlers from './filters_js/filters-handlers.js';
import FilterSearch from './filters_js/filters-search.js';
import FilterSort from './filters_js/filters-sort.js';
import FilterUI from './filters_js/filters-ui.js';
// Убрали импорт FilterSticky

// Главный менеджер фильтров
const FilterManager = {
    global: FilterGlobal,
    handlers: FilterHandlers,
    search: FilterSearch,
    sort: FilterSort,
    ui: FilterUI,
    // Убрали sticky: FilterSticky

    // Флаг для отслеживания, была ли уже скрыта заглушка
    skeletonHidden: false,

    // Метод для скрытия заглушки и показа настоящих фильтров
    hideSkeletonAndShowFilters() {
        // Предотвращаем повторное выполнение
        if (this.skeletonHidden) return;

        FilterManagerDebugTimer.start('hideSkeletonAndShowFilters');
        console.log('🔄 Hiding filters skeleton and showing real filters...');

        const skeleton = document.getElementById('filters-skeleton');
        const realFiltersContainer = document.getElementById('real-filters-container');

        if (skeleton && realFiltersContainer) {
            // Скрываем заглушку
            skeleton.classList.add('skeleton-hidden');

            // Показываем настоящие фильтры
            realFiltersContainer.style.display = 'block';

            this.skeletonHidden = true;
            console.log('✅ Filters skeleton hidden, real filters displayed');

            // Убрали инициализацию sticky кнопок
        } else {
            console.warn('⚠️ Skeleton or real filters not found:', { skeleton, realFiltersContainer });
        }
        FilterManagerDebugTimer.end('hideSkeletonAndShowFilters');
    },

    // Проверка, все ли модули инициализированы и фильтры отрисованы
    checkAllModulesInitialized() {
        console.log('🔍 Checking if all modules are initialized...');

        // Проверяем, что основные модули существуют
        const modulesReady = (
            this.global &&
            this.handlers &&
            this.search &&
            this.sort &&
            this.ui
        );

        if (!modulesReady) {
            console.log('⏳ Modules not ready yet');
            return false;
        }

        // Проверяем, что фильтры отрендерены
        const filterContainers = document.querySelectorAll(
            '.platform-grid, .genre-grid, .keyword-grid, .theme-grid, ' +
            '.perspective-grid, .game-mode-grid, .engine-grid, ' +
            '.search-platform-grid, .search-genre-grid, .search-keyword-grid, ' +
            '.search-theme-grid, .search-perspective-grid, .search-game-mode-grid, ' +
            '.search-engine-grid'
        );

        if (filterContainers.length === 0) {
            console.log('⏳ Filter containers not found yet');
            return false;
        }

        // Проверяем, есть ли элементы в фильтрах
        const hasFilterItems = document.querySelectorAll(
            '.platform-item, .genre-item, .keyword-item, .theme-item, ' +
            '.perspective-item, .game-mode-item, .engine-item, ' +
            '.search-platform-item, .search-genre-item, .search-keyword-item, ' +
            '.search-theme-item, .search-perspective-item, .search-game-mode-item, ' +
            '.search-engine-item'
        ).length > 5; // Хотя бы несколько элементов должно быть

        if (!hasFilterItems) {
            console.log('⏳ Filter items not rendered yet');
            return false;
        }

        console.log('✅ All modules appear to be initialized and filters are rendered');
        return true;
    },

    // Инициализация всех модулей
    init() {
        FilterManagerDebugTimer.start('FilterManager.init');
        console.log('🚀 Initializing FilterManager with all modules...');

        try {
            // 1. Глобальные функции и обработчики формы
            if (FilterGlobal && typeof FilterGlobal.initializeAll === 'function') {
                FilterGlobal.initializeAll();
                console.log('✓ FilterGlobal initialized');
            }

            // 2. UI компоненты
            if (FilterUI && typeof FilterUI.initializeAllUI === 'function') {
                FilterUI.initializeAllUI();
                console.log('✓ FilterUI initialized');
            }

            // 3. Обработчики событий
            if (FilterHandlers && typeof FilterHandlers.initializeAllHandlers === 'function') {
                FilterHandlers.initializeAllHandlers();
                console.log('✓ FilterHandlers initialized');
            }

            // 4. Поиск в фильтрах
            if (FilterSearch && typeof FilterSearch.setupSearchFilters === 'function') {
                FilterSearch.setupSearchFilters();
                console.log('✓ FilterSearch initialized');
            }

            // 5. Сортировка (ключевой момент)
            if (FilterSort && typeof FilterSort.sortFilterLists === 'function') {
                // Сортируем сразу
                FilterSort.sortFilterLists();
                console.log('✓ FilterSort initialized');

                // После сортировки проверяем готовность
                setTimeout(() => {
                    this.checkAndHideSkeleton();
                }, 300);
            }

            // 6. Keywords пагинация (инициализируется отдельно)
            setTimeout(() => {
                if (window.KeywordsPagination && typeof window.KeywordsPagination.init === 'function') {
                    window.KeywordsPagination.init();
                    console.log('✓ KeywordsPagination initialized');

                    // Еще раз проверяем после пагинации
                    setTimeout(() => {
                        this.checkAndHideSkeleton();
                    }, 200);
                }
            }, 600);

            // 7. Периодическая проверка готовности
            this.startPeriodicCheck();

        } catch (error) {
            console.error('❌ Error initializing filter modules:', error);
            // В случае ошибки всё равно показываем фильтры
            setTimeout(() => this.forceShowFilters(), 1000);
        }
        FilterManagerDebugTimer.end('FilterManager.init');
    },

    // Проверка готовности и скрытие заглушки
    checkAndHideSkeleton() {
        if (this.checkAllModulesInitialized()) {
            setTimeout(() => {
                this.hideSkeletonAndShowFilters();
            }, 200);
        }
    },

    // Периодическая проверка
    startPeriodicCheck() {
        let checkCount = 0;
        const maxChecks = 15; // Максимум 15 проверок (7.5 секунд)

        const intervalId = setInterval(() => {
            checkCount++;

            if (this.skeletonHidden) {
                clearInterval(intervalId);
                return;
            }

            if (this.checkAllModulesInitialized()) {
                this.hideSkeletonAndShowFilters();
                clearInterval(intervalId);
                return;
            }

            if (checkCount >= maxChecks) {
                console.log('⚠️ Max checks reached, forcing filters to show');
                this.forceShowFilters();
                clearInterval(intervalId);
            }
        }, 500);
    },

    // Принудительно показать фильтры
    forceShowFilters() {
        console.log('⚠️ Force showing filters...');

        const skeleton = document.getElementById('filters-skeleton');
        const realFiltersContainer = document.getElementById('real-filters-container');

        if (skeleton && realFiltersContainer) {
            skeleton.classList.add('skeleton-hidden');
            realFiltersContainer.style.display = 'block';
            this.skeletonHidden = true;
            console.log('✅ Filters force displayed');
        }
    },

    // Ручное обновление
    manualShowFilters() {
        this.forceShowFilters();
    }
};

// Делаем доступным глобально
window.FilterManager = FilterManager;
window.FilterGlobal = FilterGlobal;
window.FilterHandlers = FilterHandlers;
window.FilterSearch = FilterSearch;
window.FilterSort = FilterSort;
window.FilterUI = FilterUI;
// Убрали window.FilterSticky

// Глобальная функция для обратной совместимости
window.initializeFilters = function() {
    console.log('🔄 Calling initializeFilters (legacy)...');
    FilterManager.init();
};

// Автоматическая инициализация
document.addEventListener('DOMContentLoaded', function() {
    setTimeout(() => {
        if (document.querySelector('#main-search-form') ||
            document.querySelector('.games-container')) {
            console.log('⚡ Auto-initializing filters...');
            FilterManager.init();
        }
    }, 300);
});

// Проверка после полной загрузки
window.addEventListener('load', function() {
    setTimeout(() => {
        if (!FilterManager.skeletonHidden) {
            console.log('🔄 Post-load check for filters...');
            FilterManager.checkAndHideSkeleton();
        }
    }, 1000);
});

export default FilterManager;
export {
    FilterGlobal,
    FilterHandlers,
    FilterSearch,
    FilterSort,
    FilterUI
    // Убрали FilterSticky из экспорта
};