// games/static/games/js/modules/game_list_script.js

const GameListScript = {
    // Инициализация
    init() {
        console.log('GameListScript initialized');

        this.form = document.getElementById('main-search-form');
        if (!this.form) {
            console.error('Main search form not found');
            return;
        }

        // Инициализация всех компонентов
        this.initializeSearchInputs();
        this.initializeShowAllToggles();
        this.setupActiveTagRemoval();
        this.setupAutoSubmit();
        this.setupCheckboxListeners();
        this.setupDateFilterListeners();

        // Инициализация пагинации ключевых слов
        this.setupKeywordsPagination();

        console.log('GameListScript initialization completed');
    },

    // Инициализация пагинации ключевых слов
    setupKeywordsPagination() {
        // Проверяем, нужно ли показывать пагинацию
        const keywordItems = document.querySelectorAll('.keyword-item');
        if (keywordItems.length > 30 && window.KeywordsPagination) {
            // Инициализируем с небольшой задержкой
            setTimeout(() => {
                if (window.KeywordsPagination && typeof window.KeywordsPagination.init === 'function') {
                    window.KeywordsPagination.init();
                }
            }, 200);
        }
    },

    // Инициализация полей поиска (исправленная версия)
    initializeSearchInputs() {
        console.log('Initializing search inputs...');

        const searchConfigs = [
            { id: 'genre-search', selector: '.genre-item', attr: 'data-genre-name' },
            { id: 'keyword-search', selector: '.keyword-item', attr: 'data-keyword-name' },
            { id: 'platform-search', selector: '.platform-item', attr: 'data-platform-name' },
            { id: 'theme-search', selector: '.theme-item', attr: 'data-theme-name' },
            { id: 'perspective-search', selector: '.perspective-item', attr: 'data-perspective-name' },
            { id: 'developer-search', selector: '.developer-item', attr: 'data-developer-name' },
            { id: 'game-mode-search', selector: '.game-mode-item', attr: 'data-game-mode-name' },
            { id: 'game-type-search', selector: '.game-type-item', attr: 'data-game-type-name' }
        ];

        searchConfigs.forEach(({ id, selector, attr }) => {
            this.setupSearchInput(id, selector, attr);
        });
    },

    // Настройка одного поля поиска (исправленная версия)
    setupSearchInput(inputId, itemSelector, nameAttribute) {
        const searchInput = document.getElementById(inputId);
        if (!searchInput) return;

        // Сохраняем исходный элемент
        const originalInput = searchInput;

        // Функция обработки ввода
        const handleInput = (e) => {
            const searchTerm = e.target.value.toLowerCase().trim();
            const items = document.querySelectorAll(itemSelector);

            items.forEach(item => {
                const itemName = item.getAttribute(nameAttribute).toLowerCase();
                const isMatch = itemName.includes(searchTerm);
                item.style.display = isMatch ? 'block' : 'none';
            });

            // Особый случай: для ключевых слов обновляем пагинацию
            if (inputId === 'keyword-search' && window.KeywordsPagination) {
                if (window.KeywordsPagination.updateAfterSearch) {
                    window.KeywordsPagination.updateAfterSearch();
                }
            }
        };

        // Создаем новый элемент с обработчиками
        const newInput = originalInput.cloneNode(false);

        // Копируем все атрибуты
        for (let attr of originalInput.attributes) {
            newInput.setAttribute(attr.name, attr.value);
        }

        // Копируем значение
        newInput.value = originalInput.value;

        // Добавляем обработчики
        newInput.addEventListener('input', handleInput);
        newInput.addEventListener('search', () => {
            if (newInput.value === '') {
                // Особый случай: для ключевых слов восстанавливаем пагинацию
                if (inputId === 'keyword-search' && window.KeywordsPagination) {
                    if (window.KeywordsPagination.forceUpdate) {
                        setTimeout(() => {
                            window.KeywordsPagination.forceUpdate();
                        }, 50);
                    }
                }

                newInput.dispatchEvent(new Event('input'));
            }
        });

        // Заменяем элемент в том же месте
        originalInput.parentNode.replaceChild(newInput, originalInput);
    },

    // Инициализация переключателей "Show All"
    initializeShowAllToggles() {
        console.log('Initializing show all toggles...');

        const toggleConfigs = [
            {
                btnSelector: '.show-all-genres-btn',
                listSelector: '.genre-list',
                defaultHeight: '200px',
                expandedHeight: '400px'
            },
            {
                btnSelector: '.show-all-keywords-btn',
                listSelector: '.keyword-list',
                defaultHeight: '200px',
                expandedHeight: '400px'
            },
            {
                btnSelector: '.show-all-platforms-btn',
                listSelector: '.platform-list',
                defaultHeight: '200px',
                expandedHeight: '400px'
            },
            {
                btnSelector: '.show-all-themes-btn',
                listSelector: '.theme-list',
                defaultHeight: '200px',
                expandedHeight: '400px'
            },
            {
                btnSelector: '.show-all-perspectives-btn',
                listSelector: '.perspective-list',
                defaultHeight: '200px',
                expandedHeight: '400px'
            },
            {
                btnSelector: '.show-all-developers-btn',
                listSelector: '.developer-list',
                defaultHeight: '200px',
                expandedHeight: '400px'
            },
            {
                btnSelector: '.show-all-game-modes-btn',
                listSelector: '.game-mode-list',
                defaultHeight: '200px',
                expandedHeight: '400px'
            },
            {
                btnSelector: '.show-all-game-types-btn',
                listSelector: '.game-type-list',
                defaultHeight: '200px',
                expandedHeight: '400px'
            }
        ];

        toggleConfigs.forEach(({ btnSelector, listSelector, defaultHeight, expandedHeight }) => {
            this.setupShowAllToggle(btnSelector, listSelector, defaultHeight, expandedHeight);
        });
    },

    // Настройка одного переключателя "Show All"
    setupShowAllToggle(btnSelector, listSelector, defaultHeight = '200px', expandedHeight = '400px') {
        const showAllBtn = document.querySelector(btnSelector);
        const listElement = document.querySelector(listSelector);

        if (!showAllBtn || !listElement) return;

        let isExpanded = false;

        showAllBtn.addEventListener('click', () => {
            isExpanded = !isExpanded;

            if (isExpanded) {
                listElement.style.maxHeight = expandedHeight;
                const showText = showAllBtn.querySelector('.show-text');
                const hideText = showAllBtn.querySelector('.hide-text');
                if (showText) showText.style.display = 'none';
                if (hideText) hideText.style.display = 'inline';
            } else {
                listElement.style.maxHeight = defaultHeight;
                const showText = showAllBtn.querySelector('.show-text');
                const hideText = showAllBtn.querySelector('.hide-text');
                if (showText) showText.style.display = 'inline';
                if (hideText) hideText.style.display = 'none';
            }

            // Особый случай: для ключевых слов обновляем пагинацию
            if (listSelector === '.keyword-list' && window.KeywordsPagination) {
                setTimeout(() => {
                    if (window.KeywordsPagination.forceUpdate) {
                        window.KeywordsPagination.forceUpdate();
                    }
                }, 100);
            }
        });
    },

    // Удаление активных тегов
    setupActiveTagRemoval() {
        console.log('Setting up active tag removal...');

        const tagConfigs = [
            { class: 'active-genre-tag', attr: 'data-genre-id', checkboxClass: '.genre-checkbox' },
            { class: 'active-keyword-tag', attr: 'data-keyword-id', checkboxClass: '.keyword-checkbox' },
            { class: 'active-platform-tag', attr: 'data-platform-id', checkboxClass: '.platform-checkbox' },
            { class: 'active-theme-tag', attr: 'data-theme-id', checkboxClass: '.theme-checkbox' },
            { class: 'active-perspective-tag', attr: 'data-perspective-id', checkboxClass: '.perspective-checkbox' },
            { class: 'active-developer-tag', attr: 'data-developer-id', checkboxClass: '.developer-checkbox' },
            { class: 'active-game-mode-tag', attr: 'data-game-mode-id', checkboxClass: '.game-mode-checkbox' },
            { class: 'active-game-type-tag', attr: 'data-game-type-id', checkboxClass: '.game-type-checkbox' },
            { class: 'active-date-filter-tag', attr: 'data-date-range', checkboxClass: '' } // Для даты нет чекбокса
        ];

        tagConfigs.forEach(({ class: tagClass, attr, checkboxClass }) => {
            this.setupTagRemovalForType(tagClass, attr, checkboxClass);
        });
    },

    // Настройка удаления тегов для одного типа
    setupTagRemovalForType(tagClass, attr, checkboxClass) {
        document.querySelectorAll(`.${tagClass}`).forEach(tag => {
            // Удаляем старые обработчики
            const newTag = tag.cloneNode(true);
            tag.parentNode.replaceChild(newTag, tag);

            newTag.addEventListener('click', (e) => {
                e.preventDefault();
                e.stopPropagation();

                const id = newTag.getAttribute(attr);
                let checkbox = null;

                if (checkboxClass) {
                    checkbox = document.querySelector(`${checkboxClass}[value="${id}"]`);
                }

                if (checkbox) {
                    checkbox.checked = false;
                }

                // Обновляем скрытые поля и отправляем форму
                this.updateHiddenFields();

                // Сохраняем позицию прокрутки перед отправкой
                if (window.FilterManager && window.FilterManager.handlers) {
                    window.FilterManager.handlers.saveScrollPosition();
                }

                // Небольшая задержка для гарантии сохранения позиции
                setTimeout(() => {
                    this.form.submit();
                }, 50);
            });
        });
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
            { selector: '.developer-checkbox:checked', field: 'developers-field' },
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
            }
        });

        // Обновляем поля даты
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
        }

        // Обновляем поле поиска похожих игр (только для критериев похожести)
        this.updateFindSimilarField();
    },

    // Обновление поля find_similar (только для критериев похожести)
    updateFindSimilarField() {
        const findSimilarField = document.getElementById('find_similar_field');
        if (!findSimilarField) return;

        // Критерии похожести (только Genres, Keywords, Themes, Perspectives, Game Modes)
        // ИСКЛЮЧАЕМ: Platforms, Developers, Game Types, Release Date
        const similarityCriteria = [
            document.querySelectorAll('.genre-checkbox:checked').length,
            document.querySelectorAll('.keyword-checkbox:checked').length,
            document.querySelectorAll('.theme-checkbox:checked').length,
            document.querySelectorAll('.perspective-checkbox:checked').length,
            document.querySelectorAll('.game-mode-checkbox:checked').length
        ];

        const hasSimilarityCriteria = similarityCriteria.some(count => count > 0);
        findSimilarField.value = hasSimilarityCriteria ? '1' : '0';

        console.log(`Find similar field updated: ${hasSimilarityCriteria ? 'enabled' : 'disabled'}`);
    },

    // Настройка автоматической отправки
    setupAutoSubmit() {
        console.log('Setting up auto-submit...');

        // Для критериев похожести - включаем режим похожих игр при изменении
        const similarityCheckboxes = [
            '.genre-checkbox',
            '.keyword-checkbox',
            '.theme-checkbox',
            '.perspective-checkbox',
            '.game-mode-checkbox'
        ];

        similarityCheckboxes.forEach(selector => {
            document.querySelectorAll(selector).forEach(checkbox => {
                // Удаляем старые обработчики
                const newCheckbox = checkbox.cloneNode(true);
                checkbox.parentNode.replaceChild(newCheckbox, checkbox);

                newCheckbox.addEventListener('change', () => {
                    // Автоматически включаем find_similar
                    const findSimilarField = document.getElementById('find_similar_field');
                    if (findSimilarField) {
                        findSimilarField.value = '1';
                    }
                    this.updateHiddenFields(); // Только обновляем поля, не отправляем
                });
            });
        });

        // Для поисковых фильтров (Platforms, Developers, Game Types, Release Date) - не включаем find_similar
        const searchFilterCheckboxes = [
            '.platform-checkbox',
            '.developer-checkbox',
            '.game-type-checkbox'
        ];

        searchFilterCheckboxes.forEach(selector => {
            document.querySelectorAll(selector).forEach(checkbox => {
                // Удаляем старые обработчики
                const newCheckbox = checkbox.cloneNode(true);
                checkbox.parentNode.replaceChild(newCheckbox, checkbox);

                newCheckbox.addEventListener('change', () => {
                    this.updateHiddenFields(); // Только обновляем поля
                    // find_similar остается без изменений
                });
            });
        });

        // Сортировка - отправляем форму
        const sortSelect = document.querySelector('select[name="sort"]');
        if (sortSelect) {
            // Удаляем старые обработчики
            const newSortSelect = sortSelect.cloneNode(true);
            sortSelect.parentNode.replaceChild(newSortSelect, sortSelect);

            newSortSelect.addEventListener('change', () => {
                // Сохраняем позицию прокрутки
                if (window.FilterManager && window.FilterManager.handlers) {
                    window.FilterManager.handlers.saveScrollPosition();
                }

                this.form.submit();
            });
        }
    },

    // Настройка слушателей чекбоксов (для мгновенного обновления UI)
    setupCheckboxListeners() {
        console.log('Setting up checkbox listeners...');

        const allCheckboxes = document.querySelectorAll(
            '.genre-checkbox, .keyword-checkbox, .platform-checkbox, ' +
            '.theme-checkbox, .perspective-checkbox, .developer-checkbox, ' +
            '.game-mode-checkbox, .game-type-checkbox'
        );

        allCheckboxes.forEach(checkbox => {
            // Удаляем старые обработчики
            const newCheckbox = checkbox.cloneNode(true);
            checkbox.parentNode.replaceChild(newCheckbox, checkbox);

            newCheckbox.addEventListener('change', () => {
                // Обновляем скрытые поля при любом изменении чекбокса
                this.updateHiddenFields();

                // Особый случай: для ключевых слов обновляем пагинацию
                if (newCheckbox.classList.contains('keyword-checkbox') && window.KeywordsPagination) {
                    if (window.KeywordsPagination.forceUpdate) {
                        setTimeout(() => {
                            window.KeywordsPagination.forceUpdate();
                        }, 50);
                    }
                }

                // Обновляем сортировку в UI, если доступен FilterManager
                if (window.FilterManager && window.FilterManager.sort) {
                    setTimeout(() => {
                        window.FilterManager.sort.sortFilterLists();
                    }, 50);
                }
            });
        });
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
            // Удаляем старые обработчики
            const newMinSlider = minSlider.cloneNode(true);
            minSlider.parentNode.replaceChild(newMinSlider, minSlider);

            newMinSlider.addEventListener('input', () => {
                this.updateHiddenFields();
                if (window.FilterManager && window.FilterManager.sort) {
                    setTimeout(() => {
                        window.FilterManager.sort.sortFilterLists();
                    }, 50);
                }
            });
        }

        if (maxSlider) {
            // Удаляем старые обработчики
            const newMaxSlider = maxSlider.cloneNode(true);
            maxSlider.parentNode.replaceChild(newMaxSlider, maxSlider);

            newMaxSlider.addEventListener('input', () => {
                this.updateHiddenFields();
                if (window.FilterManager && window.FilterManager.sort) {
                    setTimeout(() => {
                        window.FilterManager.sort.sortFilterLists();
                    }, 50);
                }
            });
        }

        if (manualStart) {
            // Удаляем старые обработчики
            const newManualStart = manualStart.cloneNode(true);
            manualStart.parentNode.replaceChild(newManualStart, manualStart);

            newManualStart.addEventListener('change', () => {
                this.updateHiddenFields();
                if (window.FilterManager && window.FilterManager.sort) {
                    setTimeout(() => {
                        window.FilterManager.sort.sortFilterLists();
                    }, 50);
                }
            });
        }

        if (manualEnd) {
            // Удаляем старые обработчики
            const newManualEnd = manualEnd.cloneNode(true);
            manualEnd.parentNode.replaceChild(newManualEnd, manualEnd);

            newManualEnd.addEventListener('change', () => {
                this.updateHiddenFields();
                if (window.FilterManager && window.FilterManager.sort) {
                    setTimeout(() => {
                        window.FilterManager.sort.sortFilterLists();
                    }, 50);
                }
            });
        }

        // Кнопки быстрого выбора диапазона
        const quickButtons = document.querySelectorAll('#release-date-content .btn-outline-secondary');
        quickButtons.forEach(button => {
            if (button.textContent.includes('Years') || button.textContent.includes('s')) {
                // Удаляем старые обработчики
                const newButton = button.cloneNode(true);
                button.parentNode.replaceChild(newButton, button);

                newButton.addEventListener('click', () => {
                    setTimeout(() => {
                        this.updateHiddenFields();
                        if (window.FilterManager && window.FilterManager.sort) {
                            setTimeout(() => {
                                window.FilterManager.sort.sortFilterLists();
                            }, 50);
                        }
                    }, 100);
                });
            }
        });
    },

    // Восстановление выбранных чекбоксов из URL
    restoreSelectedCheckboxes() {
        console.log('Restoring selected checkboxes from URL...');

        const urlParams = new URLSearchParams(window.location.search);

        // Маппинг параметров URL на селекторы чекбоксов
        const paramMapping = [
            { param: 'g', selector: '.genre-checkbox' },
            { param: 'k', selector: '.keyword-checkbox' },
            { param: 'p', selector: '.platform-checkbox' },
            { param: 't', selector: '.theme-checkbox' },
            { param: 'pp', selector: '.perspective-checkbox' },
            { param: 'd', selector: '.developer-checkbox' },
            { param: 'gm', selector: '.game-mode-checkbox' },
            { param: 'gt', selector: '.game-type-checkbox' }
        ];

        paramMapping.forEach(({ param, selector }) => {
            const paramValue = urlParams.get(param);
            if (paramValue) {
                const ids = paramValue.split(',').map(id => id.trim());
                ids.forEach(id => {
                    const checkbox = document.querySelector(`${selector}[value="${id}"]`);
                    if (checkbox) {
                        checkbox.checked = true;
                    }
                });
            }
        });

        // Восстанавливаем фильтр даты
        const yearRange = urlParams.get('yr');
        const yearStart = urlParams.get('ys');
        const yearEnd = urlParams.get('ye');

        if (yearRange) {
            const parts = yearRange.split('-');
            if (parts.length === 2) {
                const yearStartField = document.getElementById('year-start-field');
                const yearEndField = document.getElementById('year-end-field');
                if (yearStartField && yearEndField) {
                    yearStartField.value = parts[0];
                    yearEndField.value = parts[1];

                    // Обновляем ползунки
                    const minSlider = document.getElementById('year-range-slider-min');
                    const maxSlider = document.getElementById('year-range-slider-max');
                    if (minSlider) minSlider.value = parts[0];
                    if (maxSlider) maxSlider.value = parts[1];

                    // Обновляем отображаемые значения
                    const minValueSpan = document.getElementById('min-year-value');
                    const maxValueSpan = document.getElementById('max-year-value');
                    if (minValueSpan) minValueSpan.textContent = parts[0];
                    if (maxValueSpan) maxValueSpan.textContent = parts[1];

                    // Обновляем поля ручного ввода
                    const manualStart = document.getElementById('manual-year-start');
                    const manualEnd = document.getElementById('manual-year-end');
                    if (manualStart) manualStart.value = parts[0];
                    if (manualEnd) manualEnd.value = parts[1];
                }
            }
        } else if (yearStart || yearEnd) {
            const yearStartField = document.getElementById('year-start-field');
            const yearEndField = document.getElementById('year-end-field');
            if (yearStartField) yearStartField.value = yearStart;
            if (yearEndField) yearEndField.value = yearEnd;

            // Обновляем ползунки если значения есть
            if (yearStart) {
                const minSlider = document.getElementById('year-range-slider-min');
                if (minSlider) minSlider.value = yearStart;

                const minValueSpan = document.getElementById('min-year-value');
                if (minValueSpan) minValueSpan.textContent = yearStart;

                const manualStart = document.getElementById('manual-year-start');
                if (manualStart) manualStart.value = yearStart;
            }

            if (yearEnd) {
                const maxSlider = document.getElementById('year-range-slider-max');
                if (maxSlider) maxSlider.value = yearEnd;

                const maxValueSpan = document.getElementById('max-year-value');
                if (maxValueSpan) maxValueSpan.textContent = yearEnd;

                const manualEnd = document.getElementById('manual-year-end');
                if (manualEnd) manualEnd.value = yearEnd;
            }
        }

        // Восстанавливаем find_similar
        const findSimilar = urlParams.get('find_similar');
        const findSimilarField = document.getElementById('find_similar_field');
        if (findSimilarField) {
            findSimilarField.value = findSimilar === '1' ? '1' : '0';
        }

        // Обновляем скрытые поля после восстановления
        this.updateHiddenFields();

        // Особый случай: для ключевых слов инициализируем пагинацию после восстановления
        setTimeout(() => {
            this.setupKeywordsPagination();
        }, 200);
    }
};

// Автоматическая инициализация при загрузке DOM
document.addEventListener('DOMContentLoaded', () => {
    // Инициализируем с задержкой для гарантии полной загрузки DOM
    setTimeout(() => {
        GameListScript.init();
        // Восстанавливаем выбранные чекбоксы из URL
        GameListScript.restoreSelectedCheckboxes();

        // Связываем с FilterManager если он существует
        if (window.FilterManager) {
            window.FilterManager.gameList = GameListScript;
        }
    }, 100);
});

export default GameListScript;