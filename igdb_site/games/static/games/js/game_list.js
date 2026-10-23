// games/static/games/js/game_list.js
// Ядро страницы списка игр: утилиты, точка входа, глобальный экспорт.
// UI-переключатели фильтров — в game_list_ui.js.
// Сбор параметров/отправка/очистки — в game_list_filters.js.

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

// Функция для динамической загрузки скриптов
function loadScript(src, callback) {
    console.log(`Loading script: ${src}`);

    const script = document.createElement('script');
    script.src = src;

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

function isGamesPage() {
    return document.querySelector('.games-container') !== null ||
           document.getElementById('games-results-container') !== null ||
           document.querySelector('.card.mb-4') !== null;
}

// ===== ОСНОВНАЯ ФУНКЦИЯ ИНИЦИАЛИЗАЦИИ =====
function initializeAllModules() {
    GameListDebugTimer.start('initializeAllModules');
    console.log('Initializing all modules for game list page...');

    // UI-переключатели (game_list_ui.js)
    initFilterTabs();
    initSectionToggles();
    initShowAllToggles();
    initSearchInputs();
    initDateFilters();
    initBadgeToggles();

    // Сбор параметров/отправка/очистки (game_list_filters.js)
    initClearButtons();
    initFormSubmit();
    initApplySortSubmit();

    loadScript('/static/games/js/game_list/ajax-pagination.js', function() {
        console.log('Ajax pagination loaded');
    });

    loadScript('/static/games/js/game_list/similarity-display.js', function() {
        console.log('Similarity display loaded');
    });

    loadScript('/static/games/js/game_list/similarity-compare.js', function() {
        console.log('Similarity compare loaded');
    });

    GameListDebugTimer.end('initializeAllModules');
}

document.addEventListener('DOMContentLoaded', function() {
    console.log('Game list page DOM loaded');

    if (isGamesPage()) {
        console.log('This is a games page, initializing modules...');
        setTimeout(() => {
            initializeAllModules();
        }, 100);
    } else {
        console.log('This is not a games page, skipping initialization');
    }
});

window.initializeGameListPage = initializeAllModules;

if (typeof module !== 'undefined' && module.exports) {
    module.exports = {
        initializeAllModules,
        isGamesPage
    };
}