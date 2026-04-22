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

// ===== ФУНКЦИЯ ДЛЯ ОТПРАВКИ ФИЛЬТРОВ =====
function initFormSubmit() {
    console.log('Initializing form submit handler...');

    const form = document.getElementById('main-search-form');
    if (!form) {
        console.log('Form not found');
        return;
    }

    const applyButton = form.querySelector('.apply-filters-main');
    if (!applyButton) {
        console.log('Apply button (.apply-filters-main) not found');
        return;
    }

    const newButton = applyButton.cloneNode(true);
    applyButton.parentNode.replaceChild(newButton, applyButton);

    newButton.addEventListener('click', function(e) {
        e.preventDefault();
        console.log('Apply filters clicked');

        const params = new URLSearchParams();

        // ===== SEARCH FILTERS (с префиксом search_) =====

        // Платформы (search_p)
        const searchPlatforms = document.querySelectorAll('.search-platform-checkbox:checked');
        if (searchPlatforms.length > 0) {
            params.set('search_p', Array.from(searchPlatforms).map(cb => cb.value).join(','));
        }

        // Типы игр (search_gt)
        const searchGameTypes = document.querySelectorAll('.search-game-type-checkbox:checked');
        if (searchGameTypes.length > 0) {
            params.set('search_gt', Array.from(searchGameTypes).map(cb => cb.value).join(','));
        }

        // Жанры (search_g)
        const searchGenres = document.querySelectorAll('.search-genre-checkbox:checked');
        if (searchGenres.length > 0) {
            params.set('search_g', Array.from(searchGenres).map(cb => cb.value).join(','));
        }

        // Ключевые слова (search_k)
        const searchKeywords = document.querySelectorAll('.search-keyword-checkbox:checked');
        if (searchKeywords.length > 0) {
            params.set('search_k', Array.from(searchKeywords).map(cb => cb.value).join(','));
        }

        // Темы (search_t)
        const searchThemes = document.querySelectorAll('.search-theme-checkbox:checked');
        if (searchThemes.length > 0) {
            params.set('search_t', Array.from(searchThemes).map(cb => cb.value).join(','));
        }

        // Перспективы (search_pp)
        const searchPerspectives = document.querySelectorAll('.search-perspective-checkbox:checked');
        if (searchPerspectives.length > 0) {
            params.set('search_pp', Array.from(searchPerspectives).map(cb => cb.value).join(','));
        }

        // Режимы игры (search_gm)
        const searchGameModes = document.querySelectorAll('.search-game-mode-checkbox:checked');
        if (searchGameModes.length > 0) {
            params.set('search_gm', Array.from(searchGameModes).map(cb => cb.value).join(','));
        }

        // Движки (search_e)
        const searchEngines = document.querySelectorAll('.search-engine-checkbox:checked');
        if (searchEngines.length > 0) {
            params.set('search_e', Array.from(searchEngines).map(cb => cb.value).join(','));
        }

        // Дата (search_ys, search_ye)
        const searchYearStart = document.getElementById('search-manual-year-start')?.value;
        const searchYearEnd = document.getElementById('search-manual-year-end')?.value;
        if (searchYearStart && searchYearStart !== '') params.set('search_ys', searchYearStart);
        if (searchYearEnd && searchYearEnd !== '') params.set('search_ye', searchYearEnd);

        // ===== SIMILARITY FILTERS (без префикса) =====

        // Жанры похожести (g)
        const similarityGenres = document.querySelectorAll('.genre-checkbox:checked');
        if (similarityGenres.length > 0) {
            params.set('g', Array.from(similarityGenres).map(cb => cb.value).join(','));
        }

        // Ключевые слова похожести (k)
        const similarityKeywords = document.querySelectorAll('.keyword-checkbox:checked');
        if (similarityKeywords.length > 0) {
            params.set('k', Array.from(similarityKeywords).map(cb => cb.value).join(','));
        }

        // Темы похожести (t)
        const similarityThemes = document.querySelectorAll('.theme-checkbox:checked');
        if (similarityThemes.length > 0) {
            params.set('t', Array.from(similarityThemes).map(cb => cb.value).join(','));
        }

        // Перспективы похожести (pp)
        const similarityPerspectives = document.querySelectorAll('.perspective-checkbox:checked');
        if (similarityPerspectives.length > 0) {
            params.set('pp', Array.from(similarityPerspectives).map(cb => cb.value).join(','));
        }

        // Режимы игры похожести (gm)
        const similarityGameModes = document.querySelectorAll('.game-mode-checkbox:checked');
        if (similarityGameModes.length > 0) {
            params.set('gm', Array.from(similarityGameModes).map(cb => cb.value).join(','));
        }

        // Движки похожести (e)
        const similarityEngines = document.querySelectorAll('.engine-checkbox:checked');
        if (similarityEngines.length > 0) {
            params.set('e', Array.from(similarityEngines).map(cb => cb.value).join(','));
        }

        // Source game
        const sourceGameInput = document.getElementById('server-source-game-id');
        if (sourceGameInput && sourceGameInput.value) {
            params.set('source_game', sourceGameInput.value);
        }

        // Включаем режим похожести
        const hasSimilarity = similarityGenres.length > 0 || similarityKeywords.length > 0 ||
                              similarityThemes.length > 0 || similarityPerspectives.length > 0 ||
                              similarityGameModes.length > 0 || similarityEngines.length > 0 ||
                              (sourceGameInput && sourceGameInput.value);

        if (hasSimilarity) {
            params.set('find_similar', '1');
        }

        // Сортировка
        const sortSelect = document.querySelector('select[name="sort"]');
        if (sortSelect && sortSelect.value) {
            params.set('sort', sortSelect.value);
        }

        // Страница 1
        params.set('page', '1');

        const newUrl = window.location.pathname + '?' + params.toString();
        console.log('Final URL:', newUrl);
        window.location.href = newUrl;
    });
}

// ===== ФУНКЦИИ ДЛЯ ПЕРЕКЛЮЧЕНИЯ ВКЛАДОК ФИЛЬТРОВ =====
function initFilterTabs() {
    console.log('Initializing filter tabs...');

    // Ищем кастомные вкладки из _real_filters.html
    const tabLinks = document.querySelectorAll('.filter-tab-link');
    const tabPanes = document.querySelectorAll('.filter-tab-pane');

    console.log('Found .filter-tab-link:', tabLinks.length);
    console.log('Found .filter-tab-pane:', tabPanes.length);

    if (tabLinks.length === 0) {
        console.log('No custom filter tabs found');
        return;
    }

    // Скрываем все панели
    tabPanes.forEach(pane => {
        pane.style.display = 'none';
    });

    // Функция активации вкладки
    function activateTab(tabId) {
        console.log('Activating tab:', tabId);

        tabLinks.forEach(link => {
            link.classList.remove('active');
        });

        tabPanes.forEach(pane => {
            pane.style.display = 'none';
        });

        const activeLink = Array.from(tabLinks).find(link => {
            return link.getAttribute('data-tab') === tabId;
        });

        if (activeLink) {
            activeLink.classList.add('active');
        }

        const activePane = document.getElementById(tabId);
        if (activePane) {
            activePane.style.display = 'block';
        }

        try {
            localStorage.setItem('active_filter_tab', tabId);
        } catch(e) {}
    }

    // Добавляем обработчики
    tabLinks.forEach(link => {
        link.addEventListener('click', function(e) {
            e.preventDefault();
            const tabId = this.getAttribute('data-tab');
            if (tabId) {
                activateTab(tabId);
            }
        });
    });

    // Восстанавливаем сохранённую вкладку
    const savedTab = localStorage.getItem('active_filter_tab');
    if (savedTab === 'search-filters-pane' || savedTab === 'similarity-filters-pane') {
        activateTab(savedTab);
    } else {
        // По умолчанию search-filters-pane
        activateTab('search-filters-pane');
    }
}

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

// ===== ФУНКЦИИ ДЛЯ ПЕРЕКЛЮЧЕНИЯ СЕКЦИЙ ФИЛЬТРОВ =====
function initSectionToggles() {
    console.log('Initializing section toggles...');

    function saveSectionState(sectionId, isOpen) {
        try {
            localStorage.setItem(`filter_section_${sectionId}`, isOpen ? 'open' : 'closed');
        } catch (e) {
            console.warn('Could not save section state:', e);
        }
    }

    function saveSubsectionState(subsectionId, isOpen) {
        try {
            localStorage.setItem(`filter_subsection_${subsectionId}`, isOpen ? 'open' : 'closed');
        } catch (e) {
            console.warn('Could not save subsection state:', e);
        }
    }

    function getSectionState(sectionId) {
        try {
            return localStorage.getItem(`filter_section_${sectionId}`);
        } catch (e) {
            return null;
        }
    }

    function getSubsectionState(subsectionId) {
        try {
            return localStorage.getItem(`filter_subsection_${subsectionId}`);
        } catch (e) {
            return null;
        }
    }

    // Переключение главных секций
    document.querySelectorAll('.toggle-section').forEach(section => {
        const targetId = section.getAttribute('data-target');
        const targetContent = document.getElementById(targetId);
        const icon = section.querySelector('i');

        if (targetContent && icon) {
            const savedState = getSectionState(targetId);

            if (savedState === 'open') {
                targetContent.style.display = 'block';
                icon.classList.remove('bi-chevron-right');
                icon.classList.add('bi-chevron-down');
            } else if (savedState === 'closed') {
                targetContent.style.display = 'none';
                icon.classList.remove('bi-chevron-down');
                icon.classList.add('bi-chevron-right');
            } else {
                // Нет сохраненного состояния - оставляем как есть (CSS уже скрыл)
                // Просто обновляем иконку
                icon.classList.remove('bi-chevron-down');
                icon.classList.add('bi-chevron-right');
            }
        }

        section.addEventListener('click', function(e) {
            e.preventDefault();
            const targetId = this.getAttribute('data-target');
            const targetContent = document.getElementById(targetId);
            const icon = this.querySelector('i');

            if (!targetContent || !icon) return;

            if (targetContent.style.display === 'none' || targetContent.style.display === '') {
                targetContent.style.display = 'block';
                icon.classList.remove('bi-chevron-right');
                icon.classList.add('bi-chevron-down');
                saveSectionState(targetId, true);
            } else {
                targetContent.style.display = 'none';
                icon.classList.remove('bi-chevron-down');
                icon.classList.add('bi-chevron-right');
                saveSectionState(targetId, false);
            }
        });
    });

    // Переключение подсекций
    document.querySelectorAll('.toggle-subsection').forEach(subsection => {
        const targetId = subsection.getAttribute('data-target');
        const targetContent = document.getElementById(targetId);
        const icon = subsection.querySelector('i');

        if (targetContent && icon) {
            const savedState = getSubsectionState(targetId);

            if (savedState === 'open') {
                targetContent.style.display = 'block';
                icon.classList.remove('bi-chevron-right');
                icon.classList.add('bi-chevron-down');
            } else if (savedState === 'closed') {
                targetContent.style.display = 'none';
                icon.classList.remove('bi-chevron-down');
                icon.classList.add('bi-chevron-right');
            } else {
                // Нет сохраненного состояния - оставляем как есть (CSS уже скрыл)
                // Просто обновляем иконку
                icon.classList.remove('bi-chevron-down');
                icon.classList.add('bi-chevron-right');
            }
        }

        subsection.addEventListener('click', function(e) {
            e.preventDefault();
            const targetId = this.getAttribute('data-target');
            const targetContent = document.getElementById(targetId);
            const icon = this.querySelector('i');

            if (!targetContent || !icon) return;

            if (targetContent.style.display === 'none' || targetContent.style.display === '') {
                targetContent.style.display = 'block';
                icon.classList.remove('bi-chevron-right');
                icon.classList.add('bi-chevron-down');
                saveSubsectionState(targetId, true);
            } else {
                targetContent.style.display = 'none';
                icon.classList.remove('bi-chevron-down');
                icon.classList.add('bi-chevron-right');
                saveSubsectionState(targetId, false);
            }
        });
    });
}

// ===== ФУНКЦИИ ДЛЯ КНОПОК "SHOW ALL" =====
function initShowAllToggles() {
    console.log('Initializing show all toggles...');

    const toggles = [
        { btn: '.show-all-platforms-btn', list: '.platform-list', type: 'platforms' },
        { btn: '.show-all-game-types-btn', list: '.game-type-list', type: 'game-types' },
        { btn: '.show-all-genres-btn', list: '.genre-list', type: 'genres' },
        { btn: '.show-all-keywords-btn', list: '.keyword-list', type: 'keywords' },
        { btn: '.show-all-themes-btn', list: '.theme-list', type: 'themes' },
        { btn: '.show-all-perspectives-btn', list: '.perspective-list', type: 'perspectives' },
        { btn: '.show-all-game-modes-btn', list: '.game-mode-list', type: 'game-modes' },
        { btn: '.show-all-engines-btn', list: '.engine-list', type: 'engines' },
        // Search filters
        { btn: '.search-show-all-platforms-btn', list: '.search-platform-list', type: 'search-platforms' },
        { btn: '.search-show-all-game-types-btn', list: '.search-game-type-list', type: 'search-game-types' },
        { btn: '.search-show-all-genres-btn', list: '.search-genre-list', type: 'search-genres' },
        { btn: '.search-show-all-keywords-btn', list: '.search-keyword-list', type: 'search-keywords' },
        { btn: '.search-show-all-themes-btn', list: '.search-theme-list', type: 'search-themes' },
        { btn: '.search-show-all-perspectives-btn', list: '.search-perspective-list', type: 'search-perspectives' },
        { btn: '.search-show-all-game-modes-btn', list: '.search-game-mode-list', type: 'search-game-modes' },
        { btn: '.search-show-all-engines-btn', list: '.search-engine-list', type: 'search-engines' }
    ];

    function saveState(type, isExpanded) {
        try {
            localStorage.setItem(`show_all_${type}`, isExpanded ? 'expanded' : 'collapsed');
        } catch (e) {}
    }

    function getState(type) {
        try {
            return localStorage.getItem(`show_all_${type}`) === 'expanded';
        } catch (e) {
            return false;
        }
    }

    toggles.forEach(({ btn, list, type }) => {
        const button = document.querySelector(btn);
        const listElement = document.querySelector(list);

        if (button && listElement) {
            const isExpanded = getState(type);

            if (isExpanded) {
                listElement.style.maxHeight = 'none';
                const showText = button.querySelector('.show-text');
                const hideText = button.querySelector('.hide-text');
                if (showText) showText.style.display = 'none';
                if (hideText) hideText.style.display = 'inline';
            } else {
                listElement.style.maxHeight = '200px';
            }

            button.addEventListener('click', (e) => {
                e.preventDefault();

                const currentIsExpanded = listElement.style.maxHeight === 'none' || listElement.style.maxHeight === '';

                if (currentIsExpanded) {
                    listElement.style.maxHeight = '200px';
                    const showText = button.querySelector('.show-text');
                    const hideText = button.querySelector('.hide-text');
                    if (showText) showText.style.display = 'inline';
                    if (hideText) hideText.style.display = 'none';
                    saveState(type, false);
                } else {
                    listElement.style.maxHeight = 'none';
                    const showText = button.querySelector('.show-text');
                    const hideText = button.querySelector('.hide-text');
                    if (showText) showText.style.display = 'none';
                    if (hideText) hideText.style.display = 'inline';
                    saveState(type, true);
                }
            });
        }
    });
}

// ===== ФУНКЦИИ ДЛЯ ПОИСКА В ФИЛЬТРАХ =====
function initSearchInputs() {
    console.log('Initializing search inputs...');

    const searchConfigs = [
        { inputId: 'platform-search', itemSelector: '.platform-item', dataAttr: 'data-platform-name' },
        { inputId: 'genre-search', itemSelector: '.genre-item', dataAttr: 'data-genre-name' },
        { inputId: 'keyword-search', itemSelector: '.keyword-item', dataAttr: 'data-keyword-name' },
        { inputId: 'theme-search', itemSelector: '.theme-item', dataAttr: 'data-theme-name' },
        { inputId: 'perspective-search', itemSelector: '.perspective-item', dataAttr: 'data-perspective-name' },
        { inputId: 'game-mode-search', itemSelector: '.game-mode-item', dataAttr: 'data-game-mode-name' },
        { inputId: 'engine-search', itemSelector: '.engine-item', dataAttr: 'data-engine-name' },
        { inputId: 'game-type-search', itemSelector: '.game-type-item', dataAttr: 'data-game-type-name' },
        // Search filters
        { inputId: 'search-platform-search', itemSelector: '.search-platform-item', dataAttr: 'data-platform-name' },
        { inputId: 'search-genre-search', itemSelector: '.search-genre-item', dataAttr: 'data-genre-name' },
        { inputId: 'search-keyword-search', itemSelector: '.search-keyword-item', dataAttr: 'data-keyword-name' },
        { inputId: 'search-theme-search', itemSelector: '.search-theme-item', dataAttr: 'data-theme-name' },
        { inputId: 'search-perspective-search', itemSelector: '.search-perspective-item', dataAttr: 'data-perspective-name' },
        { inputId: 'search-game-mode-search', itemSelector: '.search-game-mode-item', dataAttr: 'data-game-mode-name' },
        { inputId: 'search-engine-search', itemSelector: '.search-engine-item', dataAttr: 'data-engine-name' },
        { inputId: 'game-type-search', itemSelector: '.search-game-type-item', dataAttr: 'data-game-type-name' }
    ];

    searchConfigs.forEach(({ inputId, itemSelector, dataAttr }) => {
        const searchInput = document.getElementById(inputId);
        if (!searchInput) return;

        const handleInput = (e) => {
            const searchTerm = e.target.value.toLowerCase().trim();
            const items = document.querySelectorAll(itemSelector);

            items.forEach(item => {
                const itemName = item.getAttribute(dataAttr);
                if (itemName && (searchTerm.length === 0 || itemName.toLowerCase().includes(searchTerm))) {
                    item.style.display = 'block';
                } else {
                    item.style.display = 'none';
                }
            });
        };

        searchInput.addEventListener('input', handleInput);
        searchInput.addEventListener('search', () => {
            if (searchInput.value === '') {
                setTimeout(() => {
                    const items = document.querySelectorAll(itemSelector);
                    items.forEach(item => {
                        item.style.display = 'block';
                    });
                }, 10);
            }
        });
    });
}

// ===== ФУНКЦИИ ДЛЯ ФИЛЬТРА ДАТЫ =====
function initDateFilters() {
    console.log('Initializing date filters...');

    let minYear = window.minYear || 1970;
    let maxYear = window.maxYear || new Date().getFullYear();
    let currentYear = window.currentYear || new Date().getFullYear();

    // Search date filter
    window.updateSearchYearRange = function(type) {
        const minSlider = document.getElementById('search-year-range-slider-min');
        const maxSlider = document.getElementById('search-year-range-slider-max');
        if (!minSlider || !maxSlider) return;

        let minValue = parseInt(minSlider.value);
        let maxValue = parseInt(maxSlider.value);

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
    };

    // Similarity date filter
    window.updateYearRange = function(type) {
        const minSlider = document.getElementById('year-range-slider-min');
        const maxSlider = document.getElementById('year-range-slider-max');
        if (!minSlider || !maxSlider) return;

        let minValue = parseInt(minSlider.value);
        let maxValue = parseInt(maxSlider.value);

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
    };
}

// ===== ФУНКЦИИ ДЛЯ КНОПОК SHOW MORE/LESS (BADGES) =====
function initBadgeToggles() {
    console.log('Initializing badge toggles...');

    // Similarity filters
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

    // Search filters badges
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
}

// ===== ФУНКЦИИ ДЛЯ ОЧИСТКИ ФИЛЬТРОВ =====
function initClearButtons() {
    console.log('Initializing clear buttons...');

    document.querySelectorAll('[data-param]').forEach(button => {
        button.addEventListener('click', function(e) {
            e.preventDefault();
            const param = this.getAttribute('data-param');
            const url = new URL(window.location.href);

            const paramsToClear = param.split(',');
            paramsToClear.forEach(p => {
                if (url.searchParams.has(p)) {
                    url.searchParams.delete(p);
                }
            });

            window.location.href = url.toString();
        });
    });

    // Active tag removal
    document.addEventListener('click', (e) => {
        let tagElement = null;

        if (e.target.classList && e.target.classList.contains('active-genre-tag')) {
            tagElement = e.target;
        } else if (e.target.closest && e.target.closest('.active-genre-tag')) {
            tagElement = e.target.closest('.active-genre-tag');
        }

        if (tagElement) {
            e.preventDefault();
            const param = 'g';
            const id = tagElement.getAttribute('data-genre-id');
            const url = new URL(window.location.href);

            if (url.searchParams.has(param)) {
                const currentValues = url.searchParams.get(param).split(',');
                const newValues = currentValues.filter(value => value !== id);

                if (newValues.length > 0) {
                    url.searchParams.set(param, newValues.join(','));
                } else {
                    url.searchParams.delete(param);
                }
            }

            window.location.href = url.toString();
        }
    });
}

// ===== ОСНОВНАЯ ФУНКЦИЯ ИНИЦИАЛИЗАЦИИ =====
function initializeAllModules() {
    GameListDebugTimer.start('initializeAllModules');
    console.log('Initializing all modules for game list page...');

    // Инициализируем все UI компоненты фильтров
    initFilterTabs();
    initSectionToggles();
    initShowAllToggles();
    initSearchInputs();
    initDateFilters();
    initBadgeToggles();
    initClearButtons();
    initFormSubmit();

    // Загружаем дополнительные модули
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

// Проверяем, находимся ли мы на странице с играми
function isGamesPage() {
    return document.querySelector('.games-container') !== null ||
           document.getElementById('games-results-container') !== null ||
           document.querySelector('.card.mb-4') !== null;
}

// Инициализация при загрузке DOM
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

// Глобальная функция для ручной инициализации
window.initializeGameListPage = initializeAllModules;

// Экспортируем для использования в других модулях
if (typeof module !== 'undefined' && module.exports) {
    module.exports = {
        initializeAllModules,
        isGamesPage
    };
}