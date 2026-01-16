// games/static/games/js/modules/filters-handlers.js
const FilterHandlers = {
    // Глобальные переменные
    form: null,
    debounceTimer: null,

    init() {
        console.log('FilterHandlers init...');
        this.form = document.getElementById('main-search-form');
        // Убираем автоматическое восстановление прокрутки здесь
        // чтобы не было конфликта с filters_script.js
    },

    // Инициализация восстановления прокрутки - теперь в filters_script.js
    initializeScrollRestoration() {
        // Пустая функция - восстановление теперь в filters_script.js
        console.log('Scroll restoration moved to filters_script.js');
    },

    // Сохранение позиции прокрутки
    saveScrollPosition() {
        try {
            const scrollY = window.scrollY || document.documentElement.scrollTop;
            // Сохраняем только если не в самом верху
            if (scrollY > 100) {
                sessionStorage.setItem('filterScrollY', scrollY.toString());
                console.log('Scroll position saved:', scrollY);
            } else {
                sessionStorage.removeItem('filterScrollY');
            }
        } catch (e) {
            console.warn('Could not save scroll position:', e);
        }
    },

    // Восстановление позиции прокрутки - теперь в filters_script.js
    restoreScrollPosition() {
        // Пустая функция - восстановление теперь в filters_script.js
    },

    // Восстановление выбранных чекбоксов
    restoreSelectedCheckboxes() {
        console.log('Restoring selected checkboxes...');

        const checkboxTypes = [
            { selector: '.genre-checkbox', name: 'Genre' },
            { selector: '.keyword-checkbox', name: 'Keyword' },
            { selector: '.platform-checkbox', name: 'Platform' },
            { selector: '.theme-checkbox', name: 'Theme' },
            { selector: '.perspective-checkbox', name: 'Perspective' },
            { selector: '.game-mode-checkbox', name: 'Game Mode' },
            { selector: '.game-type-checkbox', name: 'Game Type' }
        ];

        checkboxTypes.forEach(({ selector, name }) => {
            const checkboxes = document.querySelectorAll(selector);
            const checked = Array.from(checkboxes).filter(cb => cb.checked);
            console.log(`${name}: ${checked.length} checked of ${checkboxes.length}`);
        });

        console.log('Checkbox restoration completed');
    },

    // Обновление скрытых полей формы
    updateHiddenFields() {
        console.log('Updating hidden fields...');

        // Маппинг полей
        const fieldMapping = [
            { selector: '.genre-checkbox:checked', field: 'genres-field' },
            { selector: '.keyword-checkbox:checked', field: 'keywords-field' },
            { selector: '.platform-checkbox:checked', field: 'platforms-field' },
            { selector: '.theme-checkbox:checked', field: 'themes-field' },
            { selector: '.perspective-checkbox:checked', field: 'perspectives-field' },
            { selector: '.game-mode-checkbox:checked', field: 'game-modes-field' },
            { selector: '.game-type-checkbox:checked', field: 'game-types-field' }
        ];

        // Обновляем каждое поле
        fieldMapping.forEach(({ selector, field }) => {
            const checkboxes = document.querySelectorAll(selector);
            const ids = Array.from(checkboxes).map(cb => cb.value);
            const fieldElement = document.getElementById(field);
            if (fieldElement) {
                fieldElement.value = ids.join(',');
                console.log(`${field}: ${ids.length} items`);
            }
        });

        // Обновляем поля даты (если они есть)
        this.updateDateFields();

        // Обновляем поле поиска похожих игр
        this.updateFindSimilarField();
    },

    // Обновление полей даты
    updateDateFields() {
        const yearStartField = document.getElementById('year-start-field');
        const yearEndField = document.getElementById('year-end-field');
        const yearRangeField = document.getElementById('year-range-field');

        if (yearStartField && yearEndField && yearRangeField) {
            const startValue = yearStartField.value;
            const endValue = yearEndField.value;

            if (startValue && endValue) {
                yearRangeField.value = `${startValue}-${endValue}`;
            } else {
                yearRangeField.value = '';
            }

            console.log(`Date fields updated: ${startValue || 'empty'} - ${endValue || 'empty'}`);
        }
    },

    updateFindSimilarField() {
        const findSimilarField = document.getElementById('find_similar_field');
        if (!findSimilarField) return;

        // Критерии похожести (только Similarity Filters)
        const similarityCriteria = [
            document.querySelectorAll('.genre-checkbox:checked').length,
            document.querySelectorAll('.keyword-checkbox:checked').length,
            document.querySelectorAll('.theme-checkbox:checked').length,
            document.querySelectorAll('.perspective-checkbox:checked').length,
            document.querySelectorAll('.game-mode-checkbox:checked').length
        ];

        const hasSimilarityCriteria = similarityCriteria.some(count => count > 0);
        findSimilarField.value = hasSimilarityCriteria ? '1' : '0';
        console.log(`Find similar: ${hasSimilarityCriteria ? 'enabled' : 'disabled'}`);
    },

    // Настройка обработчиков чекбоксов
    setupCheckboxListeners() {
        console.log('Setting up checkbox listeners...');

        const allCheckboxes = document.querySelectorAll(
            '.genre-checkbox, .keyword-checkbox, .platform-checkbox, ' +
            '.theme-checkbox, .perspective-checkbox, .game-mode-checkbox, .game-type-checkbox'
        );

        allCheckboxes.forEach(checkbox => {
            checkbox.addEventListener('change', () => {
                console.log(`Checkbox changed: ${checkbox.value}, checked: ${checkbox.checked}`);
                this.updateHiddenFields();
                this.triggerSort();
            });
        });

        console.log(`Set up listeners for ${allCheckboxes.length} checkboxes`);
    },

    // Настройка кнопок очистки
    setupClearButtons() {
        console.log('Setting up clear buttons...');

        const clearButtons = [
            {
                selector: '.clear-genres-btn',
                param: 'g'
            },
            {
                selector: '.clear-keywords-btn',
                param: 'k'
            },
            {
                selector: '.clear-platforms-btn',
                param: 'p'
            },
            {
                selector: '.clear-themes-btn',
                param: 't'
            },
            {
                selector: '.clear-perspectives-btn',
                param: 'pp'
            },
            {
                selector: '.clear-game-modes-btn',
                param: 'gm'
            },
            {
                selector: '.clear-game-types-btn',
                param: 'gt'
            },
            {
                selector: '.clear-date-filter-btn',
                param: 'yr,ys,ye'
            }
        ];

        clearButtons.forEach(({ selector, param }) => {
            const button = document.querySelector(selector);
            if (button) {
                button.addEventListener('click', (e) => {
                    e.preventDefault();
                    e.stopPropagation();

                    console.log(`Clear all clicked for ${param}`);

                    // Сохраняем позицию прокрутки перед перезагрузкой
                    this.saveScrollPosition();

                    // Очищаем фильтр и перезагружаем
                    this.clearFilterAndReload(param);
                });
            }
        });
    },

    // Очистка фильтра и перезагрузка страницы
    clearFilterAndReload(param) {
        const url = new URL(window.location.href);

        // Обработка нескольких параметров (например, для даты: yr,ys,ye)
        const paramsToClear = param.split(',');

        paramsToClear.forEach(p => {
            if (url.searchParams.has(p)) {
                url.searchParams.delete(p);
            }
        });

        // Также удаляем find_similar если очищаются жанры, ключевые слова и т.д.
        if (['g', 'k', 't', 'pp', 'gm', 'gt'].includes(param)) {
            // Проверяем, остались ли другие критерии похожести
            const otherSimilarityParams = ['g', 'k', 't', 'pp', 'gm', 'gt']
                .filter(p => !paramsToClear.includes(p) && url.searchParams.has(p));

            if (otherSimilarityParams.length === 0) {
                url.searchParams.set('find_similar', '0');
            }
        }

        console.log(`Clearing filter ${param}, redirecting to:`, url.toString());

        // Небольшая задержка чтобы гарантировать сохранение позиции
        setTimeout(() => {
            window.location.href = url.toString();
        }, 50);
    },

    // Удаление активных тегов
    setupActiveTagRemoval() {
        console.log('Setting up active tag removal...');

        document.addEventListener('click', (e) => {
            const tagConfigs = [
                {
                    class: 'active-genre-tag',
                    attr: 'data-genre-id',
                    param: 'g'
                },
                {
                    class: 'active-keyword-tag',
                    attr: 'data-keyword-id',
                    param: 'k'
                },
                {
                    class: 'active-platform-tag',
                    attr: 'data-platform-id',
                    param: 'p'
                },
                {
                    class: 'active-theme-tag',
                    attr: 'data-theme-id',
                    param: 't'
                },
                {
                    class: 'active-perspective-tag',
                    attr: 'data-perspective-id',
                    param: 'pp'
                },
                {
                    class: 'active-game-mode-tag',
                    attr: 'data-game-mode-id',
                    param: 'gm'
                },
                {
                    class: 'active-game-type-tag',
                    attr: 'data-game-type-id',
                    param: 'gt'
                },
                {
                    class: 'active-date-filter-tag',
                    attr: 'data-date-range',
                    param: 'yr'
                }
            ];

            tagConfigs.forEach(({ class: tagClass, attr, param }) => {
                // Проверяем клик на самом теге или его дочерних элементах
                let tagElement = null;

                if (e.target.classList.contains(tagClass)) {
                    tagElement = e.target;
                } else if (e.target.closest(`.${tagClass}`)) {
                    tagElement = e.target.closest(`.${tagClass}`);
                }

                if (tagElement) {
                    e.preventDefault();
                    e.stopPropagation();

                    console.log(`Tag removal clicked for ${param}`);

                    // Сохраняем позицию прокрутки перед перезагрузкой
                    this.saveScrollPosition();

                    const id = tagElement.getAttribute(attr);

                    // Удаляем один фильтр через перезагрузку
                    this.removeSingleFilterAndReload(param, id);
                }
            });
        });
    },

    // Удаление одного фильтра и перезагрузка
    removeSingleFilterAndReload(param, id) {
        const url = new URL(window.location.href);

        // Особый случай для фильтра даты
        if (param === 'yr') {
            // Удаляем все параметры даты
            url.searchParams.delete('yr');
            url.searchParams.delete('ys');
            url.searchParams.delete('ye');
        } else if (url.searchParams.has(param)) {
            const currentValues = url.searchParams.get(param).split(',');
            const newValues = currentValues.filter(value => value !== id && value !== '');

            if (newValues.length > 0) {
                url.searchParams.set(param, newValues.join(','));
            } else {
                url.searchParams.delete(param);

                // Если это критерий похожести, отключаем find_similar
                if (['g', 'k', 't', 'pp', 'gm', 'gt'].includes(param)) {
                    const otherSimilarityParams = ['g', 'k', 't', 'pp', 'gm', 'gt']
                        .filter(p => p !== param && url.searchParams.has(p));

                    if (otherSimilarityParams.length === 0) {
                        url.searchParams.set('find_similar', '0');
                    }
                }
            }
        }

        console.log(`Removing filter ${param}=${id || 'date'}, redirecting to:`, url.toString());

        setTimeout(() => {
            window.location.href = url.toString();
        }, 50);
    },

    // Автоотправка формы для сортировки
    setupAutoSubmit() {
        console.log('Setting up auto-submit for sort...');

        const sortSelect = document.querySelector('select[name="sort"]');
        if (sortSelect && this.form) {
            sortSelect.addEventListener('change', () => {
                // Сохраняем позицию прокрутки перед отправкой формы
                this.saveScrollPosition();
                this.form.submit();
            });
        }
    },

    // Триггер сортировки с debounce
    triggerSort() {
        // Используем debounce для предотвращения множественных сортировок
        if (this.debounceTimer) {
            clearTimeout(this.debounceTimer);
        }

        this.debounceTimer = setTimeout(() => {
            if (window.FilterManager && window.FilterManager.sort &&
                typeof window.FilterManager.sort.sortFilterLists === 'function') {
                window.FilterManager.sort.sortFilterLists();
            }
            this.debounceTimer = null;
        }, 50);
    },

    // Настройка обработчиков для фильтра даты
    setupDateFilterListeners() {
        console.log('Setting up date filter listeners...');

        // Слушатели изменения ползунков
        const minSlider = document.getElementById('year-range-slider-min');
        const maxSlider = document.getElementById('year-range-slider-max');
        const manualStart = document.getElementById('manual-year-start');
        const manualEnd = document.getElementById('manual-year-end');

        if (minSlider) {
            minSlider.addEventListener('input', () => {
                this.updateDateFields();
                this.triggerSort();
            });
        }

        if (maxSlider) {
            maxSlider.addEventListener('input', () => {
                this.updateDateFields();
                this.triggerSort();
            });
        }

        if (manualStart) {
            manualStart.addEventListener('change', () => {
                this.updateDateFields();
                this.triggerSort();
            });
        }

        if (manualEnd) {
            manualEnd.addEventListener('change', () => {
                this.updateDateFields();
                this.triggerSort();
            });
        }

        // Кнопки быстрого выбора диапазона
        const quickButtons = document.querySelectorAll('#release-date-content .btn-outline-secondary');
        quickButtons.forEach(button => {
            if (button.textContent.includes('Years') || button.textContent.includes('s')) {
                button.addEventListener('click', () => {
                    setTimeout(() => {
                        this.updateDateFields();
                        this.triggerSort();
                    }, 100);
                });
            }
        });
    },

    // Инициализация всех обработчиков
    initializeAllHandlers() {
        console.log('Initializing all filter handlers...');

        try {
            this.setupCheckboxListeners();
            this.setupClearButtons();
            this.setupActiveTagRemoval();
            this.setupDateFilterListeners();
            this.setupAutoSubmit();
            this.restoreSelectedCheckboxes();
            this.updateHiddenFields();

            console.log('All filter handlers initialized successfully');
        } catch (error) {
            console.error('Error initializing filter handlers:', error);
        }
    }
};

// Авто-инициализация
document.addEventListener('DOMContentLoaded', () => {
    FilterHandlers.init();
});

export default FilterHandlers;