// games/static/games/js/game_list.js

// Добавляем служебный объект для таймеров
const GameListDebugTimer = {
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

// Основной файл для инициализации игровой страницы
// Только загружает и инициализирует модули фильтров

// Функция для динамической загрузки скриптов
function loadScript(src, callback) {
    console.log(`Loading script: ${src}`);

    const script = document.createElement('script');
    script.src = src;

    // Определяем тип скрипта по расширению
    if (src.includes('.js') && !src.includes('type=')) {
        script.type = 'module';
    }

    script.onload = function() {
        console.log(`Script loaded successfully: ${src}`);
        if (typeof callback === 'function') {
            callback();
        }
    };

    script.onerror = function() {
        console.error(`Failed to load script: ${src}`);
        // Пробуем загрузить как обычный скрипт
        const fallbackScript = document.createElement('script');
        fallbackScript.src = src;
        fallbackScript.type = 'text/javascript';

        fallbackScript.onload = function() {
            console.log(`Script loaded as fallback: ${src}`);
            if (typeof callback === 'function') {
                callback();
            }
        };

        fallbackScript.onerror = function() {
            console.error(`Failed to load script even as fallback: ${src}`);
        };

        document.head.appendChild(fallbackScript);
    };

    document.head.appendChild(script);
}

// Главная функция инициализации всех модулей
function initializeAllModules() {
    GameListDebugTimer.start('initializeAllModules');
    console.log('Initializing all modules for game list page...');

    // Загружаем модуль инициализации фильтров
    loadScript('/static/games/js/game_list/filters-init.js', function() {
        console.log('Filters init script loaded');

        // Загружаем основной модуль фильтров
        loadScript('/static/games/js/game_list/filters.js', function() {
            console.log('Filters module loaded, checking for initialization...');

            // Ждем немного для гарантии загрузки модулей
            setTimeout(() => {
                // Загружаем модуль пагинации ключевых слов
                loadScript('/static/games/js/game_list/keywords-pagination.js', function() {
                    console.log('KeywordsPagination module loaded');

                    // Инициализируем FilterManager
                    if (typeof window.FilterManager !== 'undefined' &&
                        typeof window.FilterManager.init === 'function') {
                        console.log('Initializing FilterManager...');
                        window.FilterManager.init();
                    }
                    // Пробуем глобальную функцию
                    else if (typeof window.initializeFilters === 'function') {
                        console.log('Calling global initializeFilters...');
                        window.initializeFilters();
                    }
                    // Пробуем инициализировать каждый модуль отдельно
                    else {
                        console.log('Initializing filter modules individually...');
                        initializeFilterModulesIndividually();
                    }
                });
            }, 100);
        });
    });
    GameListDebugTimer.end('initializeAllModules');
}

// Резервная инициализация модулей по отдельности
function initializeFilterModulesIndividually() {
    // Инициализируем модули в правильном порядке

    // 1. Глобальные функции
    if (typeof window.FilterGlobal !== 'undefined' &&
        typeof window.FilterGlobal.initializeAll === 'function') {
        console.log('Initializing FilterGlobal...');
        window.FilterGlobal.initializeAll();
    }

    // 2. UI компоненты
    if (typeof window.FilterUI !== 'undefined' &&
        typeof window.FilterUI.initializeAllUI === 'function') {
        console.log('Initializing FilterUI...');
        window.FilterUI.initializeAllUI();
    }

    // 3. Обработчики событий
    if (typeof window.FilterHandlers !== 'undefined' &&
        typeof window.FilterHandlers.initializeAllHandlers === 'function') {
        console.log('Initializing FilterHandlers...');
        window.FilterHandlers.initializeAllHandlers();
    }

    // 4. Поиск в фильтрах
    if (typeof window.FilterSearch !== 'undefined' &&
        typeof window.FilterSearch.setupSearchFilters === 'function') {
        console.log('Initializing FilterSearch...');
        window.FilterSearch.setupSearchFilters();
    }

    // 5. Сортировка
    if (typeof window.FilterSort !== 'undefined' &&
        typeof window.FilterSort.sortFilterLists === 'function') {
        console.log('Sorting filter lists...');
        setTimeout(() => {
            window.FilterSort.sortFilterLists();
        }, 200);
    }

    // 6. Sticky кнопки
    if (typeof window.FilterSticky !== 'undefined' &&
        typeof window.FilterSticky.init === 'function') {
        console.log('Initializing FilterSticky...');
        setTimeout(() => {
            window.FilterSticky.init();
        }, 300);
    }
}

// Проверяем, находимся ли мы на странице с фильтрами
function isFiltersPage() {
    return document.querySelector('.card.mb-4') ||
           document.getElementById('main-search-form') ||
           document.querySelector('.platform-grid') ||
           document.querySelector('.games-container');
}

// Инициализация при загрузке DOM
document.addEventListener('DOMContentLoaded', function() {
    console.log('Game list page DOM loaded');

    if (isFiltersPage()) {
        console.log('This is a filters page, initializing modules...');

        // Даем время на полную загрузку DOM
        setTimeout(() => {
            initializeAllModules();
        }, 100);
    } else {
        console.log('This is not a filters page, skipping filter initialization');
    }
});

// Глобальная функция для ручной инициализации (если нужно из других мест)
window.initializeGameListPage = initializeAllModules;

// Экспортируем для использования в других модулях (если нужно)
if (typeof module !== 'undefined' && module.exports) {
    module.exports = {
        initializeAllModules,
        initializeFilterModulesIndividually,
        isFiltersPage
    };
}