// games/static/games/js/game_list/filters-init.js
// Этот файл содержит JavaScript код, который раньше был в _filters_js.html

(function() {
    'use strict';

    console.log('Loading filters initialization script...');

    // ===== ГЛОБАЛЬНЫЕ ФУНКЦИИ ДЛЯ КНОПОК SHOW MORE/LESS =====

    window.toggleMoreplatforms = function(button) {
        const hiddenSection = document.getElementById('hidden-platforms-badges');
        const showLessBtn = button.nextElementSibling;
        if (hiddenSection) {
            hiddenSection.style.display = 'block';
            button.style.display = 'none';
            if (showLessBtn) showLessBtn.style.display = 'inline-block';

            // Сортируем после изменения
            setTimeout(() => {
                if (window.FilterManager && window.FilterManager.sort) {
                    window.FilterManager.sort.sortFilterLists();
                }
            }, 100);
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

    window.toggleMoregenres = function(button) {
        const hiddenSection = document.getElementById('hidden-genres-badges');
        const showLessBtn = button.nextElementSibling;
        if (hiddenSection) {
            hiddenSection.style.display = 'block';
            button.style.display = 'none';
            if (showLessBtn) showLessBtn.style.display = 'inline-block';

            // Сортируем после изменения
            setTimeout(() => {
                if (window.FilterManager && window.FilterManager.sort) {
                    window.FilterManager.sort.sortFilterLists();
                }
            }, 100);
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

    // ===== НОВЫЕ ФУНКЦИИ ДЛЯ ДВИЖКОВ =====
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

    // ===== ФУНКЦИИ ДЛЯ УПРАВЛЕНИЯ ФИЛЬТРОМ ДАТЫ =====

    let minYear = window.minYear || 1970;
    let maxYear = window.maxYear || new Date().getFullYear();
    let currentYear = window.currentYear || new Date().getFullYear();

    window.updateYearRange = function(type) {
        const minSlider = document.getElementById('year-range-slider-min');
        const maxSlider = document.getElementById('year-range-slider-max');
        const minValue = parseInt(minSlider.value);
        const maxValue = parseInt(maxSlider.value);

        // Гарантируем, что min <= max
        if (type === 'min' && minValue > maxValue) {
            minSlider.value = maxValue;
        }
        if (type === 'max' && maxValue < minValue) {
            maxSlider.value = minValue;
        }

        // Обновляем отображаемые значения
        const minYearValue = document.getElementById('min-year-value');
        const maxYearValue = document.getElementById('max-year-value');
        if (minYearValue) minYearValue.textContent = minSlider.value;
        if (maxYearValue) maxYearValue.textContent = maxSlider.value;

        // Обновляем поля ручного ввода
        const manualStart = document.getElementById('manual-year-start');
        const manualEnd = document.getElementById('manual-year-end');
        if (manualStart) manualStart.value = minSlider.value;
        if (manualEnd) manualEnd.value = maxSlider.value;

        // Обновляем скрытые поля формы
        const yearStartField = document.getElementById('year-start-field');
        const yearEndField = document.getElementById('year-end-field');
        if (yearStartField) yearStartField.value = minSlider.value;
        if (yearEndField) yearEndField.value = maxSlider.value;

        // Обновляем комбинированное поле
        updateCombinedYearField();
    };

    window.updateYearFromInput = function(type) {
        const startInput = document.getElementById('manual-year-start');
        const endInput = document.getElementById('manual-year-end');
        let startValue = startInput ? parseInt(startInput.value) || minYear : minYear;
        let endValue = endInput ? parseInt(endInput.value) || maxYear : maxYear;

        // Валидация значений
        if (startValue < minYear) startValue = minYear;
        if (startValue > maxYear) startValue = maxYear;
        if (endValue < minYear) endValue = minYear;
        if (endValue > maxYear) endValue = maxYear;
        if (startValue > endValue) {
            if (type === 'start') endValue = startValue;
            if (type === 'end') startValue = endValue;
        }

        // Обновляем поля
        if (startInput) startInput.value = startValue;
        if (endInput) endInput.value = endValue;

        // Обновляем ползунки
        const minSlider = document.getElementById('year-range-slider-min');
        const maxSlider = document.getElementById('year-range-slider-max');
        if (minSlider) minSlider.value = startValue;
        if (maxSlider) maxSlider.value = endValue;

        // Обновляем отображаемые значения
        const minYearValue = document.getElementById('min-year-value');
        const maxYearValue = document.getElementById('max-year-value');
        if (minYearValue) minYearValue.textContent = startValue;
        if (maxYearValue) maxYearValue.textContent = endValue;

        // Обновляем скрытые поля формы
        const yearStartField = document.getElementById('year-start-field');
        const yearEndField = document.getElementById('year-end-field');
        if (yearStartField) yearStartField.value = startValue;
        if (yearEndField) yearEndField.value = endValue;

        // Обновляем комбинированное поле
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

        // Гарантируем, что годы в пределах допустимого диапазона
        if (startYear < minYear) startYear = minYear;
        if (endYear > maxYear) endYear = maxYear;

        // Обновляем ползунки
        const minSlider = document.getElementById('year-range-slider-min');
        const maxSlider = document.getElementById('year-range-slider-max');
        if (minSlider) minSlider.value = startYear;
        if (maxSlider) maxSlider.value = endYear;

        // Обновляем отображаемые значения
        const minYearValue = document.getElementById('min-year-value');
        const maxYearValue = document.getElementById('max-year-value');
        if (minYearValue) minYearValue.textContent = startYear;
        if (maxYearValue) maxYearValue.textContent = endYear;

        // Обновляем поля ручного ввода
        const manualStart = document.getElementById('manual-year-start');
        const manualEnd = document.getElementById('manual-year-end');
        if (manualStart) manualStart.value = startYear;
        if (manualEnd) manualEnd.value = endYear;

        // Обновляем скрытые поля формы
        const yearStartField = document.getElementById('year-start-field');
        const yearEndField = document.getElementById('year-end-field');
        if (yearStartField) yearStartField.value = startYear;
        if (yearEndField) yearEndField.value = endYear;

        // Обновляем комбинированное поле
        updateCombinedYearField();
    };

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

        // Проверяем доступность модуля KeywordsPagination
        setTimeout(function() {
            if (typeof window.KeywordsPagination !== 'undefined' &&
                typeof window.KeywordsPagination.init === 'function') {
                console.log('KeywordsPagination: Module available, initializing...');
                window.KeywordsPagination.init();
            } else {
                console.log('KeywordsPagination: Module not loaded yet, trying again in 1 second...');
                // Пробуем еще раз через 1 секунду
                setTimeout(function() {
                    if (typeof window.KeywordsPagination !== 'undefined' &&
                        typeof window.KeywordsPagination.init === 'function') {
                        window.KeywordsPagination.init();
                    } else {
                        console.log('KeywordsPagination: Loading module from static files...');
                        // Пытаемся загрузить модуль
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

    // ===== CSS ПЕРЕМЕННЫЕ ДЛЯ ОРАНЖЕВОГО ЦВЕТА =====

    function ensureCSSVariables() {
        // Проверяем наличие CSS переменных для оранжевого цвета
        const style = getComputedStyle(document.documentElement);
        const primaryColor = style.getPropertyValue('--primary-color').trim();
        const secondaryColor = style.getPropertyValue('--secondary-color').trim();

        // Устанавливаем значения по умолчанию если CSS переменные не заданы
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

    // ===== ИНИЦИАЛИЗАЦИЯ ПРИ ЗАГРУЗКЕ DOM =====

    document.addEventListener('DOMContentLoaded', function() {
        console.log('Filters initialization: DOM loaded');

        // Устанавливаем CSS переменные
        ensureCSSVariables();

        // Инициализируем значения даты, если они есть
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

        // Обновляем комбинированное поле
        updateCombinedYearField();

        // Получаем данные о годах из Django
        const releaseDateFilter = document.querySelector('.release-date-filter');
        if (releaseDateFilter) {
            const minYearAttr = releaseDateFilter.getAttribute('data-min-year');
            const maxYearAttr = releaseDateFilter.getAttribute('data-max-year');
            const currentYearAttr = releaseDateFilter.getAttribute('data-current-year');

            if (minYearAttr) minYear = parseInt(minYearAttr);
            if (maxYearAttr) maxYear = parseInt(maxYearAttr);
            if (currentYearAttr) currentYear = parseInt(currentYearAttr);
        }

        // Инициализируем пагинацию ключевых слов
        setTimeout(initKeywordsPagination, 1000);

        // Добавляем стили для корректного отображения пагинации
        const existingStyles = document.getElementById('filters-init-styles');
        if (!existingStyles) {
            const styleElement = document.createElement('style');
            styleElement.id = 'filters-init-styles';
            styleElement.textContent = `
                /* Стили для корректного отображения пагинации */
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

                /* Оранжевые рамки для всех кнопок фильтров */
                .clear-keywords-btn,
                .show-more-keywords,
                .show-less-keywords,
                .show-all-keywords-btn {
                    border: 2px solid var(--secondary-color, #ff6b35) !important;
                }

                /* Стили для кнопок движков */
                .clear-engines-btn,
                .show-more-engines,
                .show-less-engines,
                .show-all-engines-btn {
                    border: 2px solid var(--secondary-color, #ff6b35) !important;
                }
            `;
            document.head.appendChild(styleElement);
        }
    });

    console.log('Filters initialization script loaded successfully');
})();