// games/static/games/js/game_list/filters_js/filters-handlers.js

// Добавляем служебный объект для таймеров
const FilterHandlersDebugTimer = {
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

const FilterHandlers = {
    // Глобальные переменные
    form: null,
    debounceTimer: null,

    init() {
        console.log('FilterHandlers init...');
        this.form = document.getElementById('main-search-form');

        // Загружаем начальные параметры из URL
        this.loadInitialParams();
    },

    // Загрузка начальных параметров из URL в скрытые поля
    loadInitialParams() {
        console.log('Loading initial params from URL...');

        const urlParams = new URLSearchParams(window.location.search);

        // Заполняем скрытые поля значениями из URL
        const fields = [
            { param: 'g', field: 'genres-field' },
            { param: 'k', field: 'keywords-field' },
            { param: 'p', field: 'platforms-field' },
            { param: 't', field: 'themes-field' },
            { param: 'pp', field: 'perspectives-field' },
            { param: 'gm', field: 'game-modes-field' },
            { param: 'gt', field: 'game-types-field' },
            { param: 'e', field: 'engines-field' },
            { param: 'ys', field: 'year-start-field' },
            { param: 'ye', field: 'year-end-field' }
        ];

        fields.forEach(({ param, field }) => {
            const value = urlParams.get(param) || '';
            const fieldElement = document.getElementById(field);
            if (fieldElement) {
                fieldElement.value = value;
                console.log(`Set ${field} = ${value}`);
            }
        });

        // Устанавливаем find_similar
        const findSimilarField = document.getElementById('find_similar_field');
        if (findSimilarField) {
            const findSimilar = urlParams.get('find_similar') || '0';
            findSimilarField.value = findSimilar;
            console.log(`Set find_similar_field = ${findSimilar}`);
        }

        // Обновляем combined year field
        this.updateDateFields();
    },

    // Сохранение позиции прокрутки
    saveScrollPosition() {
        try {
            const scrollY = window.scrollY || document.documentElement.scrollTop;
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

    // Восстановление выбранных чекбоксов
    restoreSelectedCheckboxes() {
        FilterHandlersDebugTimer.start('restoreSelectedCheckboxes');
        console.log('Restoring selected checkboxes...');

        // Проверяем Search Filters
        const searchCheckboxes = [
            { selector: '.search-genre-checkbox', name: 'Search Genre' },
            { selector: '.search-keyword-checkbox', name: 'Search Keyword' },
            { selector: '.search-platform-checkbox', name: 'Search Platform' },
            { selector: '.search-theme-checkbox', name: 'Search Theme' },
            { selector: '.search-perspective-checkbox', name: 'Search Perspective' },
            { selector: '.search-game-mode-checkbox', name: 'Search Game Mode' },
            { selector: '.search-game-type-checkbox', name: 'Search Game Type' },
            { selector: '.search-engine-checkbox', name: 'Search Engine' }
        ];

        searchCheckboxes.forEach(({ selector, name }) => {
            const checkboxes = document.querySelectorAll(selector);
            const checked = Array.from(checkboxes).filter(cb => cb.checked);
            console.log(`${name}: ${checked.length} checked of ${checkboxes.length}`);
        });

        // Проверяем Similarity Filters
        const similarityCheckboxes = [
            { selector: '.genre-checkbox', name: 'Similarity Genre' },
            { selector: '.keyword-checkbox', name: 'Similarity Keyword' },
            { selector: '.platform-checkbox', name: 'Similarity Platform' },
            { selector: '.theme-checkbox', name: 'Similarity Theme' },
            { selector: '.perspective-checkbox', name: 'Similarity Perspective' },
            { selector: '.game-mode-checkbox', name: 'Similarity Game Mode' },
            { selector: '.game-type-checkbox', name: 'Similarity Game Type' },
            { selector: '.engine-checkbox', name: 'Similarity Engine' }
        ];

        similarityCheckboxes.forEach(({ selector, name }) => {
            const checkboxes = document.querySelectorAll(selector);
            const checked = Array.from(checkboxes).filter(cb => cb.checked);
            console.log(`${name}: ${checked.length} checked of ${checkboxes.length}`);
        });

        console.log('Checkbox restoration completed');
        FilterHandlersDebugTimer.end('restoreSelectedCheckboxes');
    },

    // Получение активной вкладки
    getActiveTab() {
        const activeTabLink = document.querySelector('.filter-tab-link.active');
        if (!activeTabLink) return 'search-filters-pane';
        return activeTabLink.dataset.tab || 'search-filters-pane';
    },

    // Обновление скрытых полей формы
    updateHiddenFields() {
        FilterHandlersDebugTimer.start('updateHiddenFields');
        console.log('Updating hidden fields...');

        const activeTab = this.getActiveTab();
        console.log(`Active tab: ${activeTab}`);

        // Получаем все текущие параметры из URL один раз
        const urlParams = new URLSearchParams(window.location.search);
        const newUrlParams = new URLSearchParams();

        // Копируем существующие параметры, которые не будут изменены
        const preserveParams = ['source_game', 'sort'];
        preserveParams.forEach(param => {
            const value = urlParams.get(param);
            if (value) newUrlParams.set(param, value);
        });

        if (activeTab === 'search-filters-pane') {
            // Search Filters - обновляем ТОЛЬКО поисковые параметры

            // Платформы
            const platformCheckboxes = document.querySelectorAll('.search-platform-checkbox:checked');
            const platformIds = Array.from(platformCheckboxes).map(cb => cb.value);
            const platformsField = document.getElementById('platforms-field');
            if (platformsField) {
                platformsField.value = platformIds.join(',');
                console.log('Updated platforms-field:', platformsField.value);
            }
            if (platformIds.length > 0) {
                newUrlParams.set('p', platformIds.join(','));
            }

            // Game Types
            const gameTypeCheckboxes = document.querySelectorAll('.search-game-type-checkbox:checked');
            const gameTypeIds = Array.from(gameTypeCheckboxes).map(cb => cb.value);
            const gameTypesField = document.getElementById('game-types-field');
            if (gameTypesField) {
                gameTypesField.value = gameTypeIds.join(',');
                console.log('Updated game-types-field:', gameTypesField.value);
            }
            if (gameTypeIds.length > 0) {
                newUrlParams.set('gt', gameTypeIds.join(','));
            }

            // Даты
            const manualStart = document.getElementById('search-manual-year-start');
            const manualEnd = document.getElementById('search-manual-year-end');
            const yearStartField = document.getElementById('year-start-field');
            const yearEndField = document.getElementById('year-end-field');

            if (manualStart && manualStart.value && yearStartField) {
                yearStartField.value = manualStart.value;
                newUrlParams.set('ys', manualStart.value);
            }
            if (manualEnd && manualEnd.value && yearEndField) {
                yearEndField.value = manualEnd.value;
                newUrlParams.set('ye', manualEnd.value);
            }

            // Обновляем combined year field
            this.updateDateFields();

            // Сохраняем similarity параметры ИЗ URL (не из полей!)
            const similarityParams = ['g', 'k', 't', 'pp', 'gm', 'e'];
            similarityParams.forEach(param => {
                const value = urlParams.get(param);
                if (value) newUrlParams.set(param, value);
            });

        } else {
            // Similarity Filters - обновляем параметры похожести

            // Genres
            const genreCheckboxes = document.querySelectorAll('.genre-checkbox:checked');
            const genreIds = Array.from(genreCheckboxes).map(cb => cb.value);
            const genresField = document.getElementById('genres-field');
            if (genresField) {
                genresField.value = genreIds.join(',');
                console.log('Updated genres-field:', genresField.value);
            }
            if (genreIds.length > 0) {
                newUrlParams.set('g', genreIds.join(','));
            }

            // Keywords
            const keywordCheckboxes = document.querySelectorAll('.keyword-checkbox:checked');
            const keywordIds = Array.from(keywordCheckboxes).map(cb => cb.value);
            const keywordsField = document.getElementById('keywords-field');
            if (keywordsField) {
                keywordsField.value = keywordIds.join(',');
                console.log('Updated keywords-field:', keywordsField.value);
            }
            if (keywordIds.length > 0) {
                newUrlParams.set('k', keywordIds.join(','));
            }

            // Themes
            const themeCheckboxes = document.querySelectorAll('.theme-checkbox:checked');
            const themeIds = Array.from(themeCheckboxes).map(cb => cb.value);
            const themesField = document.getElementById('themes-field');
            if (themesField) {
                themesField.value = themeIds.join(',');
                console.log('Updated themes-field:', themesField.value);
            }
            if (themeIds.length > 0) {
                newUrlParams.set('t', themeIds.join(','));
            }

            // Perspectives
            const perspectiveCheckboxes = document.querySelectorAll('.perspective-checkbox:checked');
            const perspectiveIds = Array.from(perspectiveCheckboxes).map(cb => cb.value);
            const perspectivesField = document.getElementById('perspectives-field');
            if (perspectivesField) {
                perspectivesField.value = perspectiveIds.join(',');
                console.log('Updated perspectives-field:', perspectivesField.value);
            }
            if (perspectiveIds.length > 0) {
                newUrlParams.set('pp', perspectiveIds.join(','));
            }

            // Game Modes
            const gameModeCheckboxes = document.querySelectorAll('.game-mode-checkbox:checked');
            const gameModeIds = Array.from(gameModeCheckboxes).map(cb => cb.value);
            const gameModesField = document.getElementById('game-modes-field');
            if (gameModesField) {
                gameModesField.value = gameModeIds.join(',');
                console.log('Updated game-modes-field:', gameModesField.value);
            }
            if (gameModeIds.length > 0) {
                newUrlParams.set('gm', gameModeIds.join(','));
            }

            // Engines
            const engineCheckboxes = document.querySelectorAll('.engine-checkbox:checked');
            const engineIds = Array.from(engineCheckboxes).map(cb => cb.value);
            const enginesField = document.getElementById('engines-field');
            if (enginesField) {
                enginesField.value = engineIds.join(',');
                console.log('Updated engines-field:', enginesField.value);
            }
            if (engineIds.length > 0) {
                newUrlParams.set('e', engineIds.join(','));
            }

            // Platforms (если есть в similarity)
            const platformCheckboxes = document.querySelectorAll('.platform-checkbox:checked');
            const platformIds = Array.from(platformCheckboxes).map(cb => cb.value);
            const platformsField = document.getElementById('platforms-field');
            if (platformsField) {
                platformsField.value = platformIds.join(',');
                console.log('Updated platforms-field:', platformsField.value);
            }
            if (platformIds.length > 0) {
                newUrlParams.set('p', platformIds.join(','));
            }

            // Game Types (если есть в similarity)
            const gameTypeCheckboxes = document.querySelectorAll('.game-type-checkbox:checked');
            const gameTypeIds = Array.from(gameTypeCheckboxes).map(cb => cb.value);
            const gameTypesField = document.getElementById('game-types-field');
            if (gameTypesField) {
                gameTypesField.value = gameTypeIds.join(',');
                console.log('Updated game-types-field:', gameTypesField.value);
            }
            if (gameTypeIds.length > 0) {
                newUrlParams.set('gt', gameTypeIds.join(','));
            }
        }

        // Всегда сохраняем find_similar
        newUrlParams.set('find_similar', '1');

        // Сохраняем сортировку
        const sortSelect = document.querySelector('select[name="sort"]');
        if (sortSelect && sortSelect.value) {
            newUrlParams.set('sort', sortSelect.value);
        }

        // Сохраняем page=1 при изменении фильтров
        newUrlParams.set('page', '1');

        // Обновляем action формы с новыми параметрами
        if (this.form) {
            const baseUrl = window.location.pathname;
            const queryString = newUrlParams.toString();
            this.form.action = baseUrl + (queryString ? '?' + queryString : '');
            console.log('Form action updated to:', this.form.action);

            // Для отладки выводим все параметры
            console.log('Final URL parameters:');
            newUrlParams.forEach((value, key) => {
                console.log(`  ${key}: ${value}`);
            });
        }

        FilterHandlersDebugTimer.end('updateHiddenFields');
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

    // ИСПРАВЛЕНО: Не отключаем режим похожести
    updateFindSimilarField() {
        const findSimilarField = document.getElementById('find_similar_field');
        if (!findSimilarField) return;

        // НИКОГДА не отключаем режим похожести, если он был включен
        // Проверяем исходное значение из URL или скрытого поля
        const urlParams = new URLSearchParams(window.location.search);
        const urlFindSimilar = urlParams.get('find_similar');

        // Если в URL есть find_similar=1, сохраняем его
        if (urlFindSimilar === '1') {
            findSimilarField.value = '1';
            console.log('Find similar: сохраняем режим похожести из URL');
            return;
        }

        // Проверяем, есть ли source_game (это тоже включает режим похожести)
        const sourceGameInput = document.getElementById('server-source-game-id');
        if (sourceGameInput && sourceGameInput.value) {
            findSimilarField.value = '1';
            console.log('Find similar: сохраняем режим похожести из source_game');
            return;
        }

        // Если нет признаков режима похожести, оставляем как есть
        console.log('Find similar: значение не изменяем');
    },

    // Настройка обработчиков чекбоксов
    setupCheckboxListeners() {
        FilterHandlersDebugTimer.start('setupCheckboxListeners');
        console.log('Setting up checkbox listeners...');

        // Создаем debounced функцию один раз
        const debouncedUpdate = this.debounce(() => {
            this.updateHiddenFields();
            this.triggerSortWithDelay();
        }, 100);

        // Все чекбоксы из обеих вкладок
        const allCheckboxes = document.querySelectorAll(
            '.search-genre-checkbox, .search-keyword-checkbox, .search-platform-checkbox, ' +
            '.search-theme-checkbox, .search-perspective-checkbox, .search-game-mode-checkbox, ' +
            '.search-game-type-checkbox, .search-engine-checkbox, ' +
            '.genre-checkbox, .keyword-checkbox, .platform-checkbox, ' +
            '.theme-checkbox, .perspective-checkbox, .game-mode-checkbox, .game-type-checkbox, ' +
            '.engine-checkbox'
        );

        allCheckboxes.forEach(checkbox => {
            const newCheckbox = checkbox.cloneNode(true);
            checkbox.parentNode.replaceChild(newCheckbox, checkbox);

            newCheckbox.addEventListener('change', debouncedUpdate);
        });

        console.log(`Set up listeners for ${allCheckboxes.length} checkboxes`);
        FilterHandlersDebugTimer.end('setupCheckboxListeners');
    },

    // Вспомогательный метод debounce
    debounce(func, wait) {
        let timeout;
        return function executedFunction(...args) {
            const later = () => {
                clearTimeout(timeout);
                func(...args);
            };
            clearTimeout(timeout);
            timeout = setTimeout(later, wait);
        };
    },

    // Настройка кнопок очистки
    setupClearButtons() {
        FilterHandlersDebugTimer.start('setupClearButtons');
        console.log('Setting up clear buttons...');

        const clearButtons = [
            // Search Filters
            {
                selector: '.search-clear-genres-btn',
                param: 'g'
            },
            {
                selector: '.search-clear-keywords-btn',
                param: 'k'
            },
            {
                selector: '.search-clear-platforms-btn',
                param: 'p'
            },
            {
                selector: '.search-clear-themes-btn',
                param: 't'
            },
            {
                selector: '.search-clear-perspectives-btn',
                param: 'pp'
            },
            {
                selector: '.search-clear-game-modes-btn',
                param: 'gm'
            },
            {
                selector: '.search-clear-game-types-btn',
                param: 'gt'
            },
            {
                selector: '.search-clear-engines-btn',
                param: 'e'
            },
            {
                selector: '.search-clear-date-filter-btn',
                param: 'yr,ys,ye'
            },
            // Similarity Filters
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
                selector: '.clear-engines-btn',
                param: 'e'
            },
            {
                selector: '.clear-date-filter-btn',
                param: 'yr,ys,ye'
            }
        ];

        clearButtons.forEach(({ selector, param }) => {
            const button = document.querySelector(selector);
            if (button) {
                // Удаляем старые обработчики
                const newButton = button.cloneNode(true);
                button.parentNode.replaceChild(newButton, button);

                newButton.addEventListener('click', (e) => {
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
        FilterHandlersDebugTimer.end('setupClearButtons');
    },

    // Очистка фильтра и перезагрузка страницы
    clearFilterAndReload(param) {
        FilterHandlersDebugTimer.start('clearFilterAndReload');
        const url = new URL(window.location.href);

        // Обработка нескольких параметров (например, для даты: yr,ys,ye)
        const paramsToClear = param.split(',');

        paramsToClear.forEach(p => {
            if (url.searchParams.has(p)) {
                url.searchParams.delete(p);
            }
        });

        // Также удаляем find_similar если очищаются жанры, ключевые слова и т.д.
        if (['g', 'k', 't', 'pp', 'gm', 'gt', 'e'].includes(param)) {
            // Проверяем, остались ли другие критерии похожести
            const otherSimilarityParams = ['g', 'k', 't', 'pp', 'gm', 'gt', 'e']
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
        FilterHandlersDebugTimer.end('clearFilterAndReload');
    },

    // Удаление активных тегов
    setupActiveTagRemoval() {
        FilterHandlersDebugTimer.start('setupActiveTagRemoval');
        console.log('Setting up active tag removal...');

        document.addEventListener('click', (e) => {
            const tagConfigs = [
                // Search Filters
                {
                    class: 'search-active-genre-tag',
                    attr: 'data-genre-id',
                    param: 'g'
                },
                {
                    class: 'search-active-keyword-tag',
                    attr: 'data-keyword-id',
                    param: 'k'
                },
                {
                    class: 'search-active-platform-tag',
                    attr: 'data-platform-id',
                    param: 'p'
                },
                {
                    class: 'search-active-theme-tag',
                    attr: 'data-theme-id',
                    param: 't'
                },
                {
                    class: 'search-active-perspective-tag',
                    attr: 'data-perspective-id',
                    param: 'pp'
                },
                {
                    class: 'search-active-game-mode-tag',
                    attr: 'data-game-mode-id',
                    param: 'gm'
                },
                {
                    class: 'search-active-game-type-tag',
                    attr: 'data-game-type-id',
                    param: 'gt'
                },
                {
                    class: 'search-active-engine-tag',
                    attr: 'data-engine-id',
                    param: 'e'
                },
                {
                    class: 'search-active-date-filter-tag',
                    attr: 'data-date-range',
                    param: 'yr'
                },
                // Similarity Filters
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
                    class: 'active-engine-tag',
                    attr: 'data-engine-id',
                    param: 'e'
                },
                {
                    class: 'active-date-filter-tag',
                    attr: 'data-date-range',
                    param: 'yr'
                }
            ];

            tagConfigs.forEach(({ class: tagClass, attr, param }) => {
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
        FilterHandlersDebugTimer.end('setupActiveTagRemoval');
    },

    // Удаление одного фильтра и перезагрузка
    removeSingleFilterAndReload(param, id) {
        FilterHandlersDebugTimer.start('removeSingleFilterAndReload');
        const url = new URL(window.location.href);

        // Особый случай для фильтра даты
        if (param === 'yr') {
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

                if (['g', 'k', 't', 'pp', 'gm', 'gt', 'e'].includes(param)) {
                    const otherSimilarityParams = ['g', 'k', 't', 'pp', 'gm', 'gt', 'e']
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
        FilterHandlersDebugTimer.end('removeSingleFilterAndReload');
    },

    // Автоотправка формы для сортировки
    setupAutoSubmit() {
        console.log('Setting up auto-submit for sort...');

        const sortSelect = document.querySelector('select[name="sort"]');
        if (sortSelect && this.form) {
            const newSortSelect = sortSelect.cloneNode(true);
            sortSelect.parentNode.replaceChild(newSortSelect, sortSelect);

            newSortSelect.addEventListener('change', () => {
                this.saveScrollPosition();
                this.form.submit();
            });
        }
    },

    // Триггер сортировки с debounce
    triggerSort() {
        FilterHandlersDebugTimer.start('triggerSort');
        console.log('Triggering sort...');

        if (this.debounceTimer) {
            clearTimeout(this.debounceTimer);
        }

        this.debounceTimer = setTimeout(() => {
            if (window.FilterManager && window.FilterManager.sort) {
                console.log('Calling sortFilterLists from FilterManager');
                window.FilterManager.sort.sortFilterLists();
            } else {
                console.error('FilterManager or sort not found');
            }
            this.debounceTimer = null;
        }, 100);
        FilterHandlersDebugTimer.end('triggerSort');
    },

    triggerSortWithDelay() {
        setTimeout(() => {
            this.triggerSort();
        }, 50);
    },

    // Настройка обработчиков для фильтра даты
    setupDateFilterListeners() {
        FilterHandlersDebugTimer.start('setupDateFilterListeners');
        console.log('Setting up date filter listeners...');

        const minSlider = document.getElementById('year-range-slider-min');
        const maxSlider = document.getElementById('year-range-slider-max');
        const manualStart = document.getElementById('manual-year-start');
        const manualEnd = document.getElementById('manual-year-end');

        if (minSlider) {
            minSlider.addEventListener('input', () => {
                this.updateDateFields();
                this.triggerSortWithDelay();
            });
        }

        if (maxSlider) {
            maxSlider.addEventListener('input', () => {
                this.updateDateFields();
                this.triggerSortWithDelay();
            });
        }

        if (manualStart) {
            manualStart.addEventListener('change', () => {
                this.updateDateFields();
                this.triggerSortWithDelay();
            });
        }

        if (manualEnd) {
            manualEnd.addEventListener('change', () => {
                this.updateDateFields();
                this.triggerSortWithDelay();
            });
        }

        const quickButtons = document.querySelectorAll('#release-date-content .btn-outline-secondary');
        quickButtons.forEach(button => {
            if (button.textContent.includes('Years') || button.textContent.includes('s')) {
                button.addEventListener('click', () => {
                    setTimeout(() => {
                        this.updateDateFields();
                        this.triggerSortWithDelay();
                    }, 100);
                });
            }
        });
        FilterHandlersDebugTimer.end('setupDateFilterListeners');
    },

    // Инициализация всех обработчиков
    initializeAllHandlers() {
        FilterHandlersDebugTimer.start('FilterHandlers.initializeAllHandlers');
        console.log('Initializing all filter handlers...');

        try {
            this.setupCheckboxListeners();
            this.setupClearButtons();
            this.setupActiveTagRemoval();
            this.setupDateFilterListeners();
            this.setupAutoSubmit();
            this.restoreSelectedCheckboxes();

            // НЕТ БЛОКА С APPLY FILTERS!

            // Задержка для сортировки, чтобы чекбоксы успели восстановиться
            setTimeout(() => {
                if (window.FilterManager && window.FilterManager.sort) {
                    console.log('Initial sort after page load');
                    window.FilterManager.sort.sortFilterLists();
                }
            }, 300);

            console.log('All filter handlers initialized successfully');
        } catch (error) {
            console.error('Error initializing filter handlers:', error);
        }
        FilterHandlersDebugTimer.end('FilterHandlers.initializeAllHandlers');
    }
};

// Авто-инициализация
document.addEventListener('DOMContentLoaded', () => {
    FilterHandlers.init();
});

export default FilterHandlers;