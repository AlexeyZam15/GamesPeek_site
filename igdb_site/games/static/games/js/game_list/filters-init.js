// games/static/games/js/game_list/filters-init.js

(function() {
    'use strict';

    console.log('Loading filters initialization script...');

    // ===== ГЛОБАЛЬНЫЕ ФУНКЦИИ ДЛЯ КНОПОК SHOW MORE/LESS =====
    // (ОРИГИНАЛЬНЫЕ ДЛЯ SIMILARITY FILTERS)

    // Platforms (для Similarity)
    window.toggleMoreplatforms = function(button) {
        const hiddenSection = document.getElementById('hidden-platforms-badges');
        const showLessBtn = button.nextElementSibling;
        if (hiddenSection) {
            hiddenSection.style.display = 'block';
            button.style.display = 'none';
            if (showLessBtn) showLessBtn.style.display = 'inline-block';
        }
    };

    window.toggleLessplatforms = function(button) {
        const hiddenSection = document.getElementById('hidden-platforms-badges');
        const showMoreBtn = button.previousElementSibling;
        if (hiddenSection) {
            hiddenSection.style.display = 'none';
            button.style.display = 'none';
            if (showMoreBtn) showMoreBtn.style.display = 'inline-block';
        }
    };

    // Genres (для Similarity)
    window.toggleMoregenres = function(button) {
        const hiddenSection = document.getElementById('hidden-genres-badges');
        const showLessBtn = button.nextElementSibling;
        if (hiddenSection) {
            hiddenSection.style.display = 'block';
            button.style.display = 'none';
            if (showLessBtn) showLessBtn.style.display = 'inline-block';
        }
    };

    window.toggleLessgenres = function(button) {
        const hiddenSection = document.getElementById('hidden-genres-badges');
        const showMoreBtn = button.previousElementSibling;
        if (hiddenSection) {
            hiddenSection.style.display = 'none';
            button.style.display = 'none';
            if (showMoreBtn) showMoreBtn.style.display = 'inline-block';
        }
    };

    // Keywords (для Similarity)
    window.toggleMorekeywords = function(button) {
        const hiddenSection = document.getElementById('hidden-keywords-badges');
        const showLessBtn = button.nextElementSibling;
        if (hiddenSection) {
            hiddenSection.style.display = 'block';
            button.style.display = 'none';
            if (showLessBtn) showLessBtn.style.display = 'inline-block';
        }
    };

    window.toggleLesskeywords = function(button) {
        const hiddenSection = document.getElementById('hidden-keywords-badges');
        const showMoreBtn = button.previousElementSibling;
        if (hiddenSection) {
            hiddenSection.style.display = 'none';
            button.style.display = 'none';
            if (showMoreBtn) showMoreBtn.style.display = 'inline-block';
        }
    };

    // Themes (для Similarity)
    window.toggleMorethemes = function(button) {
        const hiddenSection = document.getElementById('hidden-themes-badges');
        const showLessBtn = button.nextElementSibling;
        if (hiddenSection) {
            hiddenSection.style.display = 'block';
            button.style.display = 'none';
            if (showLessBtn) showLessBtn.style.display = 'inline-block';
        }
    };

    window.toggleLessthemes = function(button) {
        const hiddenSection = document.getElementById('hidden-themes-badges');
        const showMoreBtn = button.previousElementSibling;
        if (hiddenSection) {
            hiddenSection.style.display = 'none';
            button.style.display = 'none';
            if (showMoreBtn) showMoreBtn.style.display = 'inline-block';
        }
    };

    // Perspectives (для Similarity)
    window.toggleMoreperspectives = function(button) {
        const hiddenSection = document.getElementById('hidden-perspectives-badges');
        const showLessBtn = button.nextElementSibling;
        if (hiddenSection) {
            hiddenSection.style.display = 'block';
            button.style.display = 'none';
            if (showLessBtn) showLessBtn.style.display = 'inline-block';
        }
    };

    window.toggleLessperspectives = function(button) {
        const hiddenSection = document.getElementById('hidden-perspectives-badges');
        const showMoreBtn = button.previousElementSibling;
        if (hiddenSection) {
            hiddenSection.style.display = 'none';
            button.style.display = 'none';
            if (showMoreBtn) showMoreBtn.style.display = 'inline-block';
        }
    };

    // Game Modes (для Similarity)
    window.toggleMoregameModes = function(button) {
        const hiddenSection = document.getElementById('hidden-game-modes-badges');
        const showLessBtn = button.nextElementSibling;
        if (hiddenSection) {
            hiddenSection.style.display = 'block';
            button.style.display = 'none';
            if (showLessBtn) showLessBtn.style.display = 'inline-block';
        }
    };

    window.toggleLessgameModes = function(button) {
        const hiddenSection = document.getElementById('hidden-game-modes-badges');
        const showMoreBtn = button.previousElementSibling;
        if (hiddenSection) {
            hiddenSection.style.display = 'none';
            button.style.display = 'none';
            if (showMoreBtn) showMoreBtn.style.display = 'inline-block';
        }
    };

    // Engines (для Similarity)
    window.toggleMoreengines = function(button) {
        const hiddenSection = document.getElementById('hidden-engines-badges');
        const showLessBtn = button.nextElementSibling;
        if (hiddenSection) {
            hiddenSection.style.display = 'block';
            button.style.display = 'none';
            if (showLessBtn) showLessBtn.style.display = 'inline-block';
        }
    };

    window.toggleLessengines = function(button) {
        const hiddenSection = document.getElementById('hidden-engines-badges');
        const showMoreBtn = button.previousElementSibling;
        if (hiddenSection) {
            hiddenSection.style.display = 'none';
            button.style.display = 'none';
            if (showMoreBtn) showMoreBtn.style.display = 'inline-block';
        }
    };

    // ===== НОВЫЕ ФУНКЦИИ ДЛЯ SEARCH FILTERS =====

    // Platforms (для Search)
    window.toggleSearchMorePlatforms = function(button) {
        const hiddenSection = document.getElementById('search-hidden-platforms-badges');
        const showLessBtn = button.nextElementSibling;
        if (hiddenSection) {
            hiddenSection.style.display = 'block';
            button.style.display = 'none';
            if (showLessBtn) showLessBtn.style.display = 'inline-block';
        }
    };

    window.toggleSearchLessPlatforms = function(button) {
        const hiddenSection = document.getElementById('search-hidden-platforms-badges');
        const showMoreBtn = button.previousElementSibling;
        if (hiddenSection) {
            hiddenSection.style.display = 'none';
            button.style.display = 'none';
            if (showMoreBtn) showMoreBtn.style.display = 'inline-block';
        }
    };

    // Genres (для Search)
    window.toggleSearchMoreGenres = function(button) {
        const hiddenSection = document.getElementById('search-hidden-genres-badges');
        const showLessBtn = button.nextElementSibling;
        if (hiddenSection) {
            hiddenSection.style.display = 'block';
            button.style.display = 'none';
            if (showLessBtn) showLessBtn.style.display = 'inline-block';
        }
    };

    window.toggleSearchLessGenres = function(button) {
        const hiddenSection = document.getElementById('search-hidden-genres-badges');
        const showMoreBtn = button.previousElementSibling;
        if (hiddenSection) {
            hiddenSection.style.display = 'none';
            button.style.display = 'none';
            if (showMoreBtn) showMoreBtn.style.display = 'inline-block';
        }
    };

    // Keywords (для Search)
    window.toggleSearchMoreKeywords = function(button) {
        const hiddenSection = document.getElementById('search-hidden-keywords-badges');
        const showLessBtn = button.nextElementSibling;
        if (hiddenSection) {
            hiddenSection.style.display = 'block';
            button.style.display = 'none';
            if (showLessBtn) showLessBtn.style.display = 'inline-block';
        }
    };

    window.toggleSearchLessKeywords = function(button) {
        const hiddenSection = document.getElementById('search-hidden-keywords-badges');
        const showMoreBtn = button.previousElementSibling;
        if (hiddenSection) {
            hiddenSection.style.display = 'none';
            button.style.display = 'none';
            if (showMoreBtn) showMoreBtn.style.display = 'inline-block';
        }
    };

    // Themes (для Search)
    window.toggleSearchMoreThemes = function(button) {
        const hiddenSection = document.getElementById('search-hidden-themes-badges');
        const showLessBtn = button.nextElementSibling;
        if (hiddenSection) {
            hiddenSection.style.display = 'block';
            button.style.display = 'none';
            if (showLessBtn) showLessBtn.style.display = 'inline-block';
        }
    };

    window.toggleSearchLessThemes = function(button) {
        const hiddenSection = document.getElementById('search-hidden-themes-badges');
        const showMoreBtn = button.previousElementSibling;
        if (hiddenSection) {
            hiddenSection.style.display = 'none';
            button.style.display = 'none';
            if (showMoreBtn) showMoreBtn.style.display = 'inline-block';
        }
    };

    // Perspectives (для Search)
    window.toggleSearchMorePerspectives = function(button) {
        const hiddenSection = document.getElementById('search-hidden-perspectives-badges');
        const showLessBtn = button.nextElementSibling;
        if (hiddenSection) {
            hiddenSection.style.display = 'block';
            button.style.display = 'none';
            if (showLessBtn) showLessBtn.style.display = 'inline-block';
        }
    };

    window.toggleSearchLessPerspectives = function(button) {
        const hiddenSection = document.getElementById('search-hidden-perspectives-badges');
        const showMoreBtn = button.previousElementSibling;
        if (hiddenSection) {
            hiddenSection.style.display = 'none';
            button.style.display = 'none';
            if (showMoreBtn) showMoreBtn.style.display = 'inline-block';
        }
    };

    // Game Modes (для Search)
    window.toggleSearchMoreGameModes = function(button) {
        const hiddenSection = document.getElementById('search-hidden-game-modes-badges');
        const showLessBtn = button.nextElementSibling;
        if (hiddenSection) {
            hiddenSection.style.display = 'block';
            button.style.display = 'none';
            if (showLessBtn) showLessBtn.style.display = 'inline-block';
        }
    };

    window.toggleSearchLessGameModes = function(button) {
        const hiddenSection = document.getElementById('search-hidden-game-modes-badges');
        const showMoreBtn = button.previousElementSibling;
        if (hiddenSection) {
            hiddenSection.style.display = 'none';
            button.style.display = 'none';
            if (showMoreBtn) showMoreBtn.style.display = 'inline-block';
        }
    };

    // Engines (для Search)
    window.toggleSearchMoreEngines = function(button) {
        const hiddenSection = document.getElementById('search-hidden-engines-badges');
        const showLessBtn = button.nextElementSibling;
        if (hiddenSection) {
            hiddenSection.style.display = 'block';
            button.style.display = 'none';
            if (showLessBtn) showLessBtn.style.display = 'inline-block';
        }
    };

    window.toggleSearchLessEngines = function(button) {
        const hiddenSection = document.getElementById('search-hidden-engines-badges');
        const showMoreBtn = button.previousElementSibling;
        if (hiddenSection) {
            hiddenSection.style.display = 'none';
            button.style.display = 'none';
            if (showMoreBtn) showMoreBtn.style.display = 'inline-block';
        }
    };

    // ===== ФУНКЦИИ ДЛЯ УПРАВЛЕНИЯ ФИЛЬТРОМ ДАТЫ (SEARCH) =====

    let minYear = window.minYear || 1970;
    let maxYear = window.maxYear || new Date().getFullYear();
    let currentYear = window.currentYear || new Date().getFullYear();

    // Функции для Search Filters
    window.updateSearchYearRange = function(type) {
        const minSlider = document.getElementById('search-year-range-slider-min');
        const maxSlider = document.getElementById('search-year-range-slider-max');
        if (!minSlider || !maxSlider) return;

        const minValue = parseInt(minSlider.value);
        const maxValue = parseInt(maxSlider.value);

        if (type === 'min' && minValue > maxValue) {
            minSlider.value = maxValue;
        }
        if (type === 'max' && maxValue < minValue) {
            maxSlider.value = minValue;
        }

        const minYearValue = document.getElementById('search-min-year-value');
        const maxYearValue = document.getElementById('search-max-year-value');
        if (minYearValue) minYearValue.textContent = minSlider.value;
        if (maxYearValue) maxYearValue.textContent = maxSlider.value;

        const manualStart = document.getElementById('search-manual-year-start');
        const manualEnd = document.getElementById('search-manual-year-end');
        if (manualStart) manualStart.value = minSlider.value;
        if (manualEnd) manualEnd.value = maxSlider.value;

        const yearStartField = document.getElementById('year-start-field');
        const yearEndField = document.getElementById('year-end-field');
        if (yearStartField) yearStartField.value = minSlider.value;
        if (yearEndField) yearEndField.value = maxSlider.value;

        updateCombinedYearField();
    };

    window.updateSearchYearFromInput = function(type) {
        const startInput = document.getElementById('search-manual-year-start');
        const endInput = document.getElementById('search-manual-year-end');
        if (!startInput || !endInput) return;

        let startValue = parseInt(startInput.value) || minYear;
        let endValue = parseInt(endInput.value) || maxYear;

        if (startValue < minYear) startValue = minYear;
        if (startValue > maxYear) startValue = maxYear;
        if (endValue < minYear) endValue = minYear;
        if (endValue > maxYear) endValue = maxYear;
        if (startValue > endValue) {
            if (type === 'start') endValue = startValue;
            if (type === 'end') startValue = endValue;
        }

        startInput.value = startValue;
        endInput.value = endValue;

        const minSlider = document.getElementById('search-year-range-slider-min');
        const maxSlider = document.getElementById('search-year-range-slider-max');
        if (minSlider) minSlider.value = startValue;
        if (maxSlider) maxSlider.value = endValue;

        const minYearValue = document.getElementById('search-min-year-value');
        const maxYearValue = document.getElementById('search-max-year-value');
        if (minYearValue) minYearValue.textContent = startValue;
        if (maxYearValue) maxYearValue.textContent = endValue;

        const yearStartField = document.getElementById('year-start-field');
        const yearEndField = document.getElementById('year-end-field');
        if (yearStartField) yearStartField.value = startValue;
        if (yearEndField) yearEndField.value = endValue;

        updateCombinedYearField();
    };

    window.setSearchYearRange = function(rangeType) {
        let startYear, endYear;

        switch(rangeType) {
            case 'last5':
                endYear = currentYear;
                startYear = currentYear - 4;
                break;
            case '2010s':
                startYear = 2010;
                endYear = 2019;
                break;
            case '2000s':
                startYear = 2000;
                endYear = 2009;
                break;
            case '1990s':
                startYear = 1990;
                endYear = 1999;
                break;
            case '1980s':
                startYear = 1980;
                endYear = 1989;
                break;
            default:
                return;
        }

        if (startYear < minYear) startYear = minYear;
        if (endYear > maxYear) endYear = maxYear;

        const minSlider = document.getElementById('search-year-range-slider-min');
        const maxSlider = document.getElementById('search-year-range-slider-max');
        if (minSlider) minSlider.value = startYear;
        if (maxSlider) maxSlider.value = endYear;

        const minYearValue = document.getElementById('search-min-year-value');
        const maxYearValue = document.getElementById('search-max-year-value');
        if (minYearValue) minYearValue.textContent = startYear;
        if (maxYearValue) maxYearValue.textContent = endYear;

        const manualStart = document.getElementById('search-manual-year-start');
        const manualEnd = document.getElementById('search-manual-year-end');
        if (manualStart) manualStart.value = startYear;
        if (manualEnd) manualEnd.value = endYear;

        const yearStartField = document.getElementById('year-start-field');
        const yearEndField = document.getElementById('year-end-field');
        if (yearStartField) yearStartField.value = startYear;
        if (yearEndField) yearEndField.value = endYear;

        updateCombinedYearField();
    };

    // Функции для Similarity Filters (оригинальные)
    window.updateYearRange = function(type) {
        const minSlider = document.getElementById('year-range-slider-min');
        const maxSlider = document.getElementById('year-range-slider-max');
        if (!minSlider || !maxSlider) return;

        const minValue = parseInt(minSlider.value);
        const maxValue = parseInt(maxSlider.value);

        if (type === 'min' && minValue > maxValue) {
            minSlider.value = maxValue;
        }
        if (type === 'max' && maxValue < minValue) {
            maxSlider.value = minValue;
        }

        const minYearValue = document.getElementById('min-year-value');
        const maxYearValue = document.getElementById('max-year-value');
        if (minYearValue) minYearValue.textContent = minSlider.value;
        if (maxYearValue) maxYearValue.textContent = maxSlider.value;

        const manualStart = document.getElementById('manual-year-start');
        const manualEnd = document.getElementById('manual-year-end');
        if (manualStart) manualStart.value = minSlider.value;
        if (manualEnd) manualEnd.value = maxSlider.value;

        const yearStartField = document.getElementById('year-start-field');
        const yearEndField = document.getElementById('year-end-field');
        if (yearStartField) yearStartField.value = minSlider.value;
        if (yearEndField) yearEndField.value = maxSlider.value;

        updateCombinedYearField();
    };

    window.updateYearFromInput = function(type) {
        const startInput = document.getElementById('manual-year-start');
        const endInput = document.getElementById('manual-year-end');
        if (!startInput || !endInput) return;

        let startValue = parseInt(startInput.value) || minYear;
        let endValue = parseInt(endInput.value) || maxYear;

        if (startValue < minYear) startValue = minYear;
        if (startValue > maxYear) startValue = maxYear;
        if (endValue < minYear) endValue = minYear;
        if (endValue > maxYear) endValue = maxYear;
        if (startValue > endValue) {
            if (type === 'start') endValue = startValue;
            if (type === 'end') startValue = endValue;
        }

        startInput.value = startValue;
        endInput.value = endValue;

        const minSlider = document.getElementById('year-range-slider-min');
        const maxSlider = document.getElementById('year-range-slider-max');
        if (minSlider) minSlider.value = startValue;
        if (maxSlider) maxSlider.value = endValue;

        const minYearValue = document.getElementById('min-year-value');
        const maxYearValue = document.getElementById('max-year-value');
        if (minYearValue) minYearValue.textContent = startValue;
        if (maxYearValue) maxYearValue.textContent = endValue;

        const yearStartField = document.getElementById('year-start-field');
        const yearEndField = document.getElementById('year-end-field');
        if (yearStartField) yearStartField.value = startValue;
        if (yearEndField) yearEndField.value = endValue;

        updateCombinedYearField();
    };

    window.setYearRange = function(rangeType) {
        let startYear, endYear;

        switch(rangeType) {
            case 'last5':
                endYear = currentYear;
                startYear = currentYear - 4;
                break;
            case '2010s':
                startYear = 2010;
                endYear = 2019;
                break;
            case '2000s':
                startYear = 2000;
                endYear = 2009;
                break;
            case '1990s':
                startYear = 1990;
                endYear = 1999;
                break;
            case '1980s':
                startYear = 1980;
                endYear = 1989;
                break;
            default:
                return;
        }

        if (startYear < minYear) startYear = minYear;
        if (endYear > maxYear) endYear = maxYear;

        const minSlider = document.getElementById('year-range-slider-min');
        const maxSlider = document.getElementById('year-range-slider-max');
        if (minSlider) minSlider.value = startYear;
        if (maxSlider) maxSlider.value = endYear;

        const minYearValue = document.getElementById('min-year-value');
        const maxYearValue = document.getElementById('max-year-value');
        if (minYearValue) minYearValue.textContent = startYear;
        if (maxYearValue) maxYearValue.textContent = endYear;

        const manualStart = document.getElementById('manual-year-start');
        const manualEnd = document.getElementById('manual-year-end');
        if (manualStart) manualStart.value = startYear;
        if (manualEnd) manualEnd.value = endYear;

        const yearStartField = document.getElementById('year-start-field');
        const yearEndField = document.getElementById('year-end-field');
        if (yearStartField) yearStartField.value = startYear;
        if (yearEndField) yearEndField.value = endYear;

        updateCombinedYearField();
    };

    function updateCombinedYearField() {
        const startField = document.getElementById('year-start-field');
        const endField = document.getElementById('year-end-field');
        const combinedField = document.getElementById('year-range-field');

        if (startField && endField && combinedField) {
            const startValue = startField.value;
            const endValue = endField.value;

            if (startValue && endValue) {
                combinedField.value = `${startValue}-${endValue}`;
            } else {
                combinedField.value = '';
            }
        }
    }

    // ===== ИНИЦИАЛИЗАЦИЯ ПАГИНАЦИИ КЛЮЧЕВЫХ СЛОВ =====

    function initKeywordsPagination() {
        console.log('Initializing keywords pagination...');

        const keywordContainer = document.querySelector('.keyword-list');
        if (!keywordContainer) {
            console.log('KeywordsPagination: Keyword container not found');
            return;
        }

        const keywordItems = keywordContainer.querySelectorAll('.keyword-item');
        const totalKeywords = keywordItems.length;

        if (totalKeywords <= 30) {
            console.log(`KeywordsPagination: Only ${totalKeywords} keywords, pagination not needed`);
            return;
        }

        console.log(`KeywordsPagination: Found ${totalKeywords} keywords, checking module...`);

        setTimeout(function() {
            if (typeof window.KeywordsPagination !== 'undefined' &&
                typeof window.KeywordsPagination.init === 'function') {
                console.log('KeywordsPagination: Module available, initializing...');
                window.KeywordsPagination.init();
            } else {
                console.log('KeywordsPagination: Module not loaded yet, trying again in 1 second...');
                setTimeout(function() {
                    if (typeof window.KeywordsPagination !== 'undefined' &&
                        typeof window.KeywordsPagination.init === 'function') {
                        window.KeywordsPagination.init();
                    } else {
                        console.log('KeywordsPagination: Loading module from static files...');
                        loadKeywordsPaginationModule();
                    }
                }, 1000);
            }
        }, 500);
    }

    function loadKeywordsPaginationModule() {
        const script = document.createElement('script');
        script.src = '/static/games/js/game_list/keywords-pagination.js';
        script.type = 'module';

        script.onload = function() {
            console.log('KeywordsPagination: Module loaded successfully');
            if (typeof window.KeywordsPagination !== 'undefined' &&
                typeof window.KeywordsPagination.init === 'function') {
                setTimeout(() => {
                    window.KeywordsPagination.init();
                }, 100);
            }
        };

        script.onerror = function() {
            console.error('KeywordsPagination: Failed to load module');
        };

        document.head.appendChild(script);
    }

    // ===== CSS ПЕРЕМЕННЫЕ =====

    function ensureCSSVariables() {
        const style = getComputedStyle(document.documentElement);
        const primaryColor = style.getPropertyValue('--primary-color').trim();
        const secondaryColor = style.getPropertyValue('--secondary-color').trim();

        if (!primaryColor || primaryColor === '') {
            document.documentElement.style.setProperty('--primary-color', '#ff6b35');
        }
        if (!secondaryColor || secondaryColor === '') {
            document.documentElement.style.setProperty('--secondary-color', '#ff6b35');
        }
        if (!style.getPropertyValue('--accent-color').trim()) {
            document.documentElement.style.setProperty('--accent-color', '#ff8e53');
        }
        if (!style.getPropertyValue('--border').trim()) {
            document.documentElement.style.setProperty('--border', 'rgba(255, 107, 53, 0.2)');
        }
        if (!style.getPropertyValue('--surface').trim()) {
            document.documentElement.style.setProperty('--surface', 'rgba(26, 26, 26, 0.4)');
        }
        if (!style.getPropertyValue('--surface-dark').trim()) {
            document.documentElement.style.setProperty('--surface-dark', 'rgba(18, 18, 18, 0.6)');
        }
    }

    // ===== ИНИЦИАЛИЗАЦИЯ =====

    document.addEventListener('DOMContentLoaded', function() {
        console.log('Filters initialization: DOM loaded');

        ensureCSSVariables();

        // Инициализируем значения даты для Search Filters
        const searchStartYearInput = document.getElementById('search-manual-year-start');
        const searchEndYearInput = document.getElementById('search-manual-year-end');

        if (searchStartYearInput && searchStartYearInput.value) {
            const yearStart = parseInt(searchStartYearInput.value);
            if (!isNaN(yearStart)) {
                const minSlider = document.getElementById('search-year-range-slider-min');
                if (minSlider) minSlider.value = yearStart;
                const minYearValue = document.getElementById('search-min-year-value');
                if (minYearValue) minYearValue.textContent = yearStart;
            }
        }

        if (searchEndYearInput && searchEndYearInput.value) {
            const yearEnd = parseInt(searchEndYearInput.value);
            if (!isNaN(yearEnd)) {
                const maxSlider = document.getElementById('search-year-range-slider-max');
                if (maxSlider) maxSlider.value = yearEnd;
                const maxYearValue = document.getElementById('search-max-year-value');
                if (maxYearValue) maxYearValue.textContent = yearEnd;
            }
        }

        // Инициализируем значения даты для Similarity Filters
        const startYearInput = document.getElementById('manual-year-start');
        const endYearInput = document.getElementById('manual-year-end');

        if (startYearInput && startYearInput.value) {
            const yearStart = parseInt(startYearInput.value);
            if (!isNaN(yearStart)) {
                const minSlider = document.getElementById('year-range-slider-min');
                if (minSlider) minSlider.value = yearStart;
                const minYearValue = document.getElementById('min-year-value');
                if (minYearValue) minYearValue.textContent = yearStart;
            }
        }

        if (endYearInput && endYearInput.value) {
            const yearEnd = parseInt(endYearInput.value);
            if (!isNaN(yearEnd)) {
                const maxSlider = document.getElementById('year-range-slider-max');
                if (maxSlider) maxSlider.value = yearEnd;
                const maxYearValue = document.getElementById('max-year-value');
                if (maxYearValue) maxYearValue.textContent = yearEnd;
            }
        }

        updateCombinedYearField();

        const releaseDateFilter = document.querySelector('.release-date-filter');
        if (releaseDateFilter) {
            const minYearAttr = releaseDateFilter.getAttribute('data-min-year');
            const maxYearAttr = releaseDateFilter.getAttribute('data-max-year');
            const currentYearAttr = releaseDateFilter.getAttribute('data-current-year');

            if (minYearAttr) minYear = parseInt(minYearAttr);
            if (maxYearAttr) maxYear = parseInt(maxYearAttr);
            if (currentYearAttr) currentYear = parseInt(currentYearAttr);
        }

        setTimeout(initKeywordsPagination, 1000);

        const existingStyles = document.getElementById('filters-init-styles');
        if (!existingStyles) {
            const styleElement = document.createElement('style');
            styleElement.id = 'filters-init-styles';
            styleElement.textContent = `
                .keyword-pagination {
                    margin-top: 1rem !important;
                    padding: 1rem !important;
                }
                .keyword-pagination .page-link,
                .keyword-pagination .page-number-btn {
                    border: 1px solid var(--border, rgba(255, 107, 53, 0.2)) !important;
                }
                .keyword-pagination .page-item.active .page-link,
                .keyword-pagination .page-number-btn.btn-primary {
                    border-color: var(--secondary-color, #ff6b35) !important;
                }
                .clear-keywords-btn,
                .show-more-keywords,
                .show-less-keywords,
                .show-all-keywords-btn {
                    border: 2px solid var(--secondary-color, #ff6b35) !important;
                }
                .clear-engines-btn,
                .show-more-engines,
                .show-less-engines,
                .show-all-engines-btn {
                    border: 2px solid var(--secondary-color, #ff6b35) !important;
                }
                .search-clear-keywords-btn,
                .search-show-more-keywords,
                .search-show-less-keywords,
                .search-show-all-keywords-btn {
                    border: 2px solid var(--secondary-color, #ff6b35) !important;
                }
                .search-clear-engines-btn,
                .search-show-more-engines,
                .search-show-less-engines,
                .search-show-all-engines-btn {
                    border: 2px solid var(--secondary-color, #ff6b35) !important;
                }
            `;
            document.head.appendChild(styleElement);
        }
    });

    console.log('Filters initialization script loaded successfully');
})();