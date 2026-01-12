// games/static/games/js/filters_script.js

document.addEventListener('DOMContentLoaded', function() {
    console.log('Filters script initialized');

    // ========== ГЛОБАЛЬНЫЕ ПЕРЕМЕННЫЕ И КОНСТАНТЫ ==========
    const form = document.getElementById('main-search-form');
    const FILTER_STORAGE_KEY = 'game_filters_state';

    // ========== ИНИЦИАЛИЗАЦИЯ СЕКЦИЙ ==========

    // Инициализация - скрываем только главные секции при загрузке
    document.querySelectorAll('.toggle-section').forEach(section => {
        const targetId = section.getAttribute('data-target');
        const targetContent = document.getElementById(targetId);

        // Главные секции скрыты по умолчанию
        if (targetContent) {
            targetContent.style.display = 'none';
            const icon = section.querySelector('i');
            icon.classList.remove('bi-chevron-down');
            icon.classList.add('bi-chevron-right');
        }
    });

    // Раскрываем все подсекции по умолчанию
    document.querySelectorAll('.toggle-subsection').forEach(subsection => {
        const targetId = subsection.getAttribute('data-target');
        const targetContent = document.getElementById(targetId);
        const icon = subsection.querySelector('i');

        if (targetContent) {
            // Подсекции раскрыты по умолчанию
            targetContent.style.display = 'block';
            icon.classList.remove('bi-chevron-right');
            icon.classList.add('bi-chevron-down');
        }
    });

    // ========== ВОССТАНОВЛЕНИЕ ВЫБРАННЫХ ЧЕКБОКСОВ ==========

    function restoreSelectedCheckboxes() {
        console.log('Restoring selected checkboxes...');

        // Восстанавливаем жанры
        const genreCheckboxes = document.querySelectorAll('.genre-checkbox');
        genreCheckboxes.forEach(checkbox => {
            const value = checkbox.value;
            const isChecked = checkbox.checked;
            console.log(`Genre ${value}: ${isChecked}`);
        });

        // Восстанавливаем ключевые слова
        const keywordCheckboxes = document.querySelectorAll('.keyword-checkbox');
        keywordCheckboxes.forEach(checkbox => {
            const value = checkbox.value;
            const isChecked = checkbox.checked;
            console.log(`Keyword ${value}: ${isChecked}`);
        });

        // Восстанавливаем платформы
        const platformCheckboxes = document.querySelectorAll('.platform-checkbox');
        platformCheckboxes.forEach(checkbox => {
            const value = checkbox.value;
            const isChecked = checkbox.checked;
            console.log(`Platform ${value}: ${isChecked}`);
        });

        // Восстанавливаем темы
        const themeCheckboxes = document.querySelectorAll('.theme-checkbox');
        themeCheckboxes.forEach(checkbox => {
            const value = checkbox.value;
            const isChecked = checkbox.checked;
            console.log(`Theme ${value}: ${isChecked}`);
        });

        // Восстанавливаем перспективы
        const perspectiveCheckboxes = document.querySelectorAll('.perspective-checkbox');
        perspectiveCheckboxes.forEach(checkbox => {
            const value = checkbox.value;
            const isChecked = checkbox.checked;
            console.log(`Perspective ${value}: ${isChecked}`);
        });

        // Восстанавливаем режимы игры
        const gameModeCheckboxes = document.querySelectorAll('.game-mode-checkbox');
        gameModeCheckboxes.forEach(checkbox => {
            const value = checkbox.value;
            const isChecked = checkbox.checked;
            console.log(`Game Mode ${value}: ${isChecked}`);
        });

        console.log('Checkbox restoration completed');
    }

    // ========== СОРТИРОВКА СПИСКОВ ФИЛЬТРОВ ==========

    function sortFilterLists() {
        console.log('Sorting filter lists...');

        // Список всех типов фильтров для обработки
        const filterTypes = [
            { container: '.genre-grid', items: '.genre-item', checkbox: '.genre-checkbox' },
            { container: '.keyword-grid', items: '.keyword-item', checkbox: '.keyword-checkbox' },
            { container: '.platform-grid', items: '.platform-item', checkbox: '.platform-checkbox' },
            { container: '.theme-grid', items: '.theme-item', checkbox: '.theme-checkbox' },
            { container: '.perspective-grid', items: '.perspective-item', checkbox: '.perspective-checkbox' },
            { container: '.game-mode-grid', items: '.game-mode-item', checkbox: '.game-mode-checkbox' }
        ];

        filterTypes.forEach(filterType => {
            const container = document.querySelector(filterType.container);
            if (!container) {
                console.log(`Container ${filterType.container} not found`);
                return;
            }

            const items = Array.from(container.querySelectorAll(filterType.items));
            console.log(`Found ${items.length} items in ${filterType.container}`);

            if (items.length === 0) return;

            // Разделяем на выбранные и невыбранные
            const selectedItems = items.filter(item => {
                const checkbox = item.querySelector(filterType.checkbox);
                return checkbox && checkbox.checked;
            });

            const unselectedItems = items.filter(item => {
                const checkbox = item.querySelector(filterType.checkbox);
                return checkbox && !checkbox.checked;
            });

            console.log(`Selected: ${selectedItems.length}, Unselected: ${unselectedItems.length} in ${filterType.container}`);

            // Оставляем невыбранные в исходном порядке (по популярности с сервера)
            // Объединяем: сначала выбранные, потом невыбранные в исходном порядке
            const sortedItems = [...selectedItems, ...unselectedItems];

            // Очищаем контейнер и добавляем отсортированные элементы
            container.innerHTML = '';
            sortedItems.forEach(item => {
                container.appendChild(item);
            });
        });
    }

    // ========== ОБНОВЛЕНИЕ СКРЫТЫХ ПОЛЕЙ ФОРМЫ ==========

    function updateHiddenFields() {
        console.log('Updating hidden fields...');

        // Обновляем жанры
        const genreCheckboxes = document.querySelectorAll('.genre-checkbox:checked');
        const genreIds = Array.from(genreCheckboxes).map(cb => cb.value);
        document.getElementById('genres-field').value = genreIds.join(',');
        console.log('Genres:', genreIds);

        // Обновляем ключевые слова
        const keywordCheckboxes = document.querySelectorAll('.keyword-checkbox:checked');
        const keywordIds = Array.from(keywordCheckboxes).map(cb => cb.value);
        document.getElementById('keywords-field').value = keywordIds.join(',');
        console.log('Keywords:', keywordIds);

        // Обновляем платформы
        const platformCheckboxes = document.querySelectorAll('.platform-checkbox:checked');
        const platformIds = Array.from(platformCheckboxes).map(cb => cb.value);
        document.getElementById('platforms-field').value = platformIds.join(',');
        console.log('Platforms:', platformIds);

        // Обновляем темы
        const themeCheckboxes = document.querySelectorAll('.theme-checkbox:checked');
        const themeIds = Array.from(themeCheckboxes).map(cb => cb.value);
        const themesField = document.getElementById('themes-field');
        if (themesField) {
            themesField.value = themeIds.join(',');
            console.log('Themes:', themeIds);
        } else {
            console.error('Themes field not found!');
        }

        // Обновляем перспективы
        const perspectiveCheckboxes = document.querySelectorAll('.perspective-checkbox:checked');
        const perspectiveIds = Array.from(perspectiveCheckboxes).map(cb => cb.value);
        const perspectivesField = document.getElementById('perspectives-field');
        if (perspectivesField) {
            perspectivesField.value = perspectiveIds.join(',');
            console.log('Perspectives:', perspectiveIds);
        } else {
            console.error('Perspectives field not found!');
        }

        // Обновляем режимы игры
        const gameModeCheckboxes = document.querySelectorAll('.game-mode-checkbox:checked');
        const gameModeIds = Array.from(gameModeCheckboxes).map(cb => cb.value);
        const gameModesField = document.getElementById('game-modes-field');
        if (gameModesField) {
            gameModesField.value = gameModeIds.join(',');
            console.log('Game modes:', gameModeIds);
        } else {
            console.error('Game modes field not found!');
        }

        // Автоматически включаем режим поиска похожих игр при выборе любых критериев похожести
        const findSimilarField = document.getElementById('find_similar_field');
        if (findSimilarField) {
            if (genreIds.length > 0 || keywordIds.length > 0 || themeIds.length > 0 ||
                perspectiveIds.length > 0 || gameModeIds.length > 0) {
                findSimilarField.value = '1';
                console.log('Find similar enabled');
            } else {
                findSimilarField.value = '0';
                console.log('Find similar disabled');
            }
        }
    }

    // ========== НАСТРОЙКА СЛУШАТЕЛЕЙ ЧЕКБОКСОВ ==========

    function setupCheckboxListeners() {
        console.log('Setting up checkbox listeners...');

        // Добавляем обработчики для всех типов чекбоксов
        const allCheckboxes = document.querySelectorAll(
            '.genre-checkbox, .keyword-checkbox, .platform-checkbox, ' +
            '.theme-checkbox, .perspective-checkbox, .game-mode-checkbox'
        );

        allCheckboxes.forEach(checkbox => {
            checkbox.addEventListener('change', function() {
                console.log(`Checkbox changed: ${this.value}, checked: ${this.checked}`);
                updateHiddenFields();
                sortFilterLists();
            });
        });

        console.log(`Set up listeners for ${allCheckboxes.length} checkboxes`);
    }

    // ========== КНОПКИ ОЧИСТКИ ==========

    function setupClearButtons() {
        console.log('Setting up clear buttons...');

        // Очистка жанров
        const clearGenresBtn = document.querySelector('.clear-genres-btn');
        if (clearGenresBtn) {
            clearGenresBtn.addEventListener('click', function(e) {
                e.preventDefault();
                document.querySelectorAll('.genre-checkbox:checked').forEach(checkbox => {
                    checkbox.checked = false;
                });
                updateHiddenFields();
                sortFilterLists();
            });
        }

        // Очистка ключевых слов
        const clearKeywordsBtn = document.querySelector('.clear-keywords-btn');
        if (clearKeywordsBtn) {
            clearKeywordsBtn.addEventListener('click', function(e) {
                e.preventDefault();
                document.querySelectorAll('.keyword-checkbox:checked').forEach(checkbox => {
                    checkbox.checked = false;
                });
                updateHiddenFields();
                sortFilterLists();
            });
        }

        // Очистка платформ
        const clearPlatformsBtn = document.querySelector('.clear-platforms-btn');
        if (clearPlatformsBtn) {
            clearPlatformsBtn.addEventListener('click', function(e) {
                e.preventDefault();
                document.querySelectorAll('.platform-checkbox:checked').forEach(checkbox => {
                    checkbox.checked = false;
                });
                updateHiddenFields();
                sortFilterLists();
            });
        }

        // Очистка тем
        const clearThemesBtn = document.querySelector('.clear-themes-btn');
        if (clearThemesBtn) {
            clearThemesBtn.addEventListener('click', function(e) {
                e.preventDefault();
                document.querySelectorAll('.theme-checkbox:checked').forEach(checkbox => {
                    checkbox.checked = false;
                });
                updateHiddenFields();
                sortFilterLists();
            });
        }

        // Очистка перспектив
        const clearPerspectivesBtn = document.querySelector('.clear-perspectives-btn');
        if (clearPerspectivesBtn) {
            clearPerspectivesBtn.addEventListener('click', function(e) {
                e.preventDefault();
                document.querySelectorAll('.perspective-checkbox:checked').forEach(checkbox => {
                    checkbox.checked = false;
                });
                updateHiddenFields();
                sortFilterLists();
            });
        }

        // Очистка режимов игры
        const clearGameModesBtn = document.querySelector('.clear-game-modes-btn');
        if (clearGameModesBtn) {
            clearGameModesBtn.addEventListener('click', function(e) {
                e.preventDefault();
                document.querySelectorAll('.game-mode-checkbox:checked').forEach(checkbox => {
                    checkbox.checked = false;
                });
                updateHiddenFields();
                sortFilterLists();
            });
        }
    }

    // ========== УДАЛЕНИЕ АКТИВНЫХ ТЕГОВ ==========

    function setupActiveTagRemoval() {
        console.log('Setting up active tag removal...');

        document.addEventListener('click', function(e) {
            // Удаление жанра
            if (e.target.classList.contains('active-genre-tag')) {
                const genreId = e.target.getAttribute('data-genre-id');
                const checkbox = document.querySelector(`.genre-checkbox[value="${genreId}"]`);
                if (checkbox) {
                    checkbox.checked = false;
                    updateHiddenFields();
                    sortFilterLists();
                }
            }

            // Удаление ключевого слова
            if (e.target.classList.contains('active-keyword-tag')) {
                const keywordId = e.target.getAttribute('data-keyword-id');
                const checkbox = document.querySelector(`.keyword-checkbox[value="${keywordId}"]`);
                if (checkbox) {
                    checkbox.checked = false;
                    updateHiddenFields();
                    sortFilterLists();
                }
            }

            // Удаление платформы
            if (e.target.classList.contains('active-platform-tag')) {
                const platformId = e.target.getAttribute('data-platform-id');
                const checkbox = document.querySelector(`.platform-checkbox[value="${platformId}"]`);
                if (checkbox) {
                    checkbox.checked = false;
                    updateHiddenFields();
                    sortFilterLists();
                }
            }

            // Удаление темы
            if (e.target.classList.contains('active-theme-tag')) {
                const themeId = e.target.getAttribute('data-theme-id');
                const checkbox = document.querySelector(`.theme-checkbox[value="${themeId}"]`);
                if (checkbox) {
                    checkbox.checked = false;
                    updateHiddenFields();
                    sortFilterLists();
                }
            }

            // Удаление перспективы
            if (e.target.classList.contains('active-perspective-tag')) {
                const perspectiveId = e.target.getAttribute('data-perspective-id');
                const checkbox = document.querySelector(`.perspective-checkbox[value="${perspectiveId}"]`);
                if (checkbox) {
                    checkbox.checked = false;
                    updateHiddenFields();
                    sortFilterLists();
                }
            }

            // Удаление режима игры
            if (e.target.classList.contains('active-game-mode-tag')) {
                const gameModeId = e.target.getAttribute('data-game-mode-id');
                const checkbox = document.querySelector(`.game-mode-checkbox[value="${gameModeId}"]`);
                if (checkbox) {
                    checkbox.checked = false;
                    updateHiddenFields();
                    sortFilterLists();
                }
            }
        });
    }

    // ========== ПОИСК ПО ФИЛЬТРАМ ==========

    function setupSearchFilters() {
        console.log('Setting up search filters...');

        // Поиск по жанрам
        const genreSearch = document.getElementById('genre-search');
        if (genreSearch) {
            genreSearch.addEventListener('input', function() {
                const searchTerm = this.value.toLowerCase();
                document.querySelectorAll('.genre-item').forEach(item => {
                    const genreName = item.getAttribute('data-genre-name');
                    if (genreName && genreName.includes(searchTerm)) {
                        item.style.display = 'block';
                    } else {
                        item.style.display = 'none';
                    }
                });
            });
        }

        // Поиск по ключевым словам
        const keywordSearch = document.getElementById('keyword-search');
        if (keywordSearch) {
            keywordSearch.addEventListener('input', function() {
                const searchTerm = this.value.toLowerCase();
                document.querySelectorAll('.keyword-item').forEach(item => {
                    const keywordName = item.getAttribute('data-keyword-name');
                    if (keywordName && keywordName.includes(searchTerm)) {
                        item.style.display = 'block';
                    } else {
                        item.style.display = 'none';
                    }
                });
            });
        }

        // Поиск по платформам
        const platformSearch = document.getElementById('platform-search');
        if (platformSearch) {
            platformSearch.addEventListener('input', function() {
                const searchTerm = this.value.toLowerCase();
                document.querySelectorAll('.platform-item').forEach(item => {
                    const platformName = item.getAttribute('data-platform-name');
                    if (platformName && platformName.includes(searchTerm)) {
                        item.style.display = 'block';
                    } else {
                        item.style.display = 'none';
                    }
                });
            });
        }

        // Поиск по темам
        const themeSearch = document.getElementById('theme-search');
        if (themeSearch) {
            themeSearch.addEventListener('input', function() {
                const searchTerm = this.value.toLowerCase();
                document.querySelectorAll('.theme-item').forEach(item => {
                    const themeName = item.getAttribute('data-theme-name');
                    if (themeName && themeName.includes(searchTerm)) {
                        item.style.display = 'block';
                    } else {
                        item.style.display = 'none';
                    }
                });
            });
        }

        // Поиск по перспективам
        const perspectiveSearch = document.getElementById('perspective-search');
        if (perspectiveSearch) {
            perspectiveSearch.addEventListener('input', function() {
                const searchTerm = this.value.toLowerCase();
                document.querySelectorAll('.perspective-item').forEach(item => {
                    const perspectiveName = item.getAttribute('data-perspective-name');
                    if (perspectiveName && perspectiveName.includes(searchTerm)) {
                        item.style.display = 'block';
                    } else {
                        item.style.display = 'none';
                    }
                });
            });
        }

        // Поиск по режимам игры
        const gameModeSearch = document.getElementById('game-mode-search');
        if (gameModeSearch) {
            gameModeSearch.addEventListener('input', function() {
                const searchTerm = this.value.toLowerCase();
                document.querySelectorAll('.game-mode-item').forEach(item => {
                    const gameModeName = item.getAttribute('data-game-mode-name');
                    if (gameModeName && gameModeName.includes(searchTerm)) {
                        item.style.display = 'block';
                    } else {
                        item.style.display = 'none';
                    }
                });
            });
        }
    }

    // ========== ПОКАЗАТЬ/СКРЫТЬ ВСЕ ==========

    function setupShowAllToggles() {
        console.log('Setting up show all toggles...');

        // Жанры
        const showAllGenresBtn = document.querySelector('.show-all-genres-btn');
        if (showAllGenresBtn) {
            showAllGenresBtn.addEventListener('click', function() {
                const genreList = document.querySelector('.genre-list');
                const showText = this.querySelector('.show-text');
                const hideText = this.querySelector('.hide-text');

                if (genreList.style.maxHeight === '200px' || genreList.style.maxHeight === '') {
                    genreList.style.maxHeight = 'none';
                    if (showText) showText.style.display = 'none';
                    if (hideText) hideText.style.display = 'inline';
                } else {
                    genreList.style.maxHeight = '200px';
                    if (showText) showText.style.display = 'inline';
                    if (hideText) hideText.style.display = 'none';
                }
            });
        }

        // Ключевые слова
        const showAllKeywordsBtn = document.querySelector('.show-all-keywords-btn');
        if (showAllKeywordsBtn) {
            showAllKeywordsBtn.addEventListener('click', function() {
                const keywordList = document.querySelector('.keyword-list');
                const showText = this.querySelector('.show-text');
                const hideText = this.querySelector('.hide-text');

                if (keywordList.style.maxHeight === '200px' || keywordList.style.maxHeight === '') {
                    keywordList.style.maxHeight = 'none';
                    if (showText) showText.style.display = 'none';
                    if (hideText) hideText.style.display = 'inline';
                } else {
                    keywordList.style.maxHeight = '200px';
                    if (showText) showText.style.display = 'inline';
                    if (hideText) hideText.style.display = 'none';
                }
            });
        }

        // Платформы
        const showAllPlatformsBtn = document.querySelector('.show-all-platforms-btn');
        if (showAllPlatformsBtn) {
            showAllPlatformsBtn.addEventListener('click', function() {
                const platformList = document.querySelector('.platform-list');
                const showText = this.querySelector('.show-text');
                const hideText = this.querySelector('.hide-text');

                if (platformList.style.maxHeight === '200px' || platformList.style.maxHeight === '') {
                    platformList.style.maxHeight = 'none';
                    if (showText) showText.style.display = 'none';
                    if (hideText) hideText.style.display = 'inline';
                } else {
                    platformList.style.maxHeight = '200px';
                    if (showText) showText.style.display = 'inline';
                    if (hideText) hideText.style.display = 'none';
                }
            });
        }

        // Темы
        const showAllThemesBtn = document.querySelector('.show-all-themes-btn');
        if (showAllThemesBtn) {
            showAllThemesBtn.addEventListener('click', function() {
                const themeList = document.querySelector('.theme-list');
                const showText = this.querySelector('.show-text');
                const hideText = this.querySelector('.hide-text');

                if (themeList.style.maxHeight === '200px' || themeList.style.maxHeight === '') {
                    themeList.style.maxHeight = 'none';
                    if (showText) showText.style.display = 'none';
                    if (hideText) hideText.style.display = 'inline';
                } else {
                    themeList.style.maxHeight = '200px';
                    if (showText) showText.style.display = 'inline';
                    if (hideText) hideText.style.display = 'none';
                }
            });
        }

        // Перспективы
        const showAllPerspectivesBtn = document.querySelector('.show-all-perspectives-btn');
        if (showAllPerspectivesBtn) {
            showAllPerspectivesBtn.addEventListener('click', function() {
                const perspectiveList = document.querySelector('.perspective-list');
                const showText = this.querySelector('.show-text');
                const hideText = this.querySelector('.hide-text');

                if (perspectiveList.style.maxHeight === '200px' || perspectiveList.style.maxHeight === '') {
                    perspectiveList.style.maxHeight = 'none';
                    if (showText) showText.style.display = 'none';
                    if (hideText) hideText.style.display = 'inline';
                } else {
                    perspectiveList.style.maxHeight = '200px';
                    if (showText) showText.style.display = 'inline';
                    if (hideText) hideText.style.display = 'none';
                }
            });
        }

        // Режимы игры
        const showAllGameModesBtn = document.querySelector('.show-all-game-modes-btn');
        if (showAllGameModesBtn) {
            showAllGameModesBtn.addEventListener('click', function() {
                const gameModeList = document.querySelector('.game-mode-list');
                const showText = this.querySelector('.show-text');
                const hideText = this.querySelector('.hide-text');

                if (gameModeList.style.maxHeight === '200px' || gameModeList.style.maxHeight === '') {
                    gameModeList.style.maxHeight = 'none';
                    if (showText) showText.style.display = 'none';
                    if (hideText) hideText.style.display = 'inline';
                } else {
                    gameModeList.style.maxHeight = '200px';
                    if (showText) showText.style.display = 'inline';
                    if (hideText) hideText.style.display = 'none';
                }
            });
        }
    }

    // ========== АВТООТПРАВКА ФОРМЫ ==========

    function setupAutoSubmit() {
        console.log('Setting up auto-submit...');

        // Для всех критериев похожести - включаем режим похожих игр
        const similarityCheckboxes = document.querySelectorAll(
            '.genre-checkbox, .keyword-checkbox, .theme-checkbox, ' +
            '.perspective-checkbox, .game-mode-checkbox'
        );

        similarityCheckboxes.forEach(checkbox => {
            checkbox.addEventListener('change', function() {
                const findSimilarField = document.getElementById('find_similar_field');
                if (findSimilarField) {
                    findSimilarField.value = '1';
                }
                updateHiddenFields();
                sortFilterLists();
            });
        });

        // Для платформ - НЕ включаем режим похожих игр
        const platformCheckboxes = document.querySelectorAll('.platform-checkbox');
        platformCheckboxes.forEach(checkbox => {
            checkbox.addEventListener('change', function() {
                updateHiddenFields();
                sortFilterLists();
            });
        });

        // Сортировка - отправляем форму при изменении
        const sortSelect = document.querySelector('select[name="sort"]');
        if (sortSelect) {
            sortSelect.addEventListener('change', function() {
                if (form) {
                    form.submit();
                }
            });
        }
    }

    // ========== ПЕРЕКЛЮЧЕНИЕ СЕКЦИЙ ==========

    function setupSectionToggles() {
        console.log('Setting up section toggles...');

        // Сохранение состояния в localStorage
        function saveSectionState(sectionId, isOpen) {
            try {
                localStorage.setItem(`filter_section_${sectionId}`, isOpen ? 'open' : 'closed');
            } catch (e) {
                console.warn('Could not save section state to localStorage:', e);
            }
        }

        function saveSubsectionState(subsectionId, isOpen) {
            try {
                localStorage.setItem(`filter_subsection_${subsectionId}`, isOpen ? 'open' : 'closed');
            } catch (e) {
                console.warn('Could not save subsection state to localStorage:', e);
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
            section.addEventListener('click', function() {
                const targetId = this.getAttribute('data-target');
                const targetContent = document.getElementById(targetId);
                const icon = this.querySelector('i');

                if (!targetContent) return;

                if (targetContent.style.display === 'none') {
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
            subsection.addEventListener('click', function() {
                const targetId = this.getAttribute('data-target');
                const targetContent = document.getElementById(targetId);
                const icon = this.querySelector('i');

                if (!targetContent) return;

                if (targetContent.style.display === 'none') {
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

        // Восстанавливаем сохраненные состояния при загрузке
        setTimeout(() => {
            // Главные секции
            document.querySelectorAll('.toggle-section').forEach(section => {
                const targetId = section.getAttribute('data-target');
                const savedState = getSectionState(targetId);
                const targetContent = document.getElementById(targetId);
                const icon = section.querySelector('i');

                if (targetContent && savedState) {
                    if (savedState === 'open') {
                        targetContent.style.display = 'block';
                        icon.classList.remove('bi-chevron-right');
                        icon.classList.add('bi-chevron-down');
                    } else {
                        targetContent.style.display = 'none';
                        icon.classList.remove('bi-chevron-down');
                        icon.classList.add('bi-chevron-right');
                    }
                }
            });

            // Подсекции
            document.querySelectorAll('.toggle-subsection').forEach(subsection => {
                const targetId = subsection.getAttribute('data-target');
                const savedState = getSubsectionState(targetId);
                const targetContent = document.getElementById(targetId);
                const icon = subsection.querySelector('i');

                if (targetContent && savedState) {
                    if (savedState === 'open') {
                        targetContent.style.display = 'block';
                        icon.classList.remove('bi-chevron-right');
                        icon.classList.add('bi-chevron-down');
                    } else {
                        targetContent.style.display = 'none';
                        icon.classList.remove('bi-chevron-down');
                        icon.classList.add('bi-chevron-right');
                    }
                }
            });
        }, 100);
    }

    // ========== ПОКАЗАТЬ/СКРЫТЬ ДОПОЛНИТЕЛЬНЫЕ БЕЙДЖИ ==========

    function setupShowMoreButtons() {
        console.log('Setting up show more/less buttons...');

        // Platforms
        window.toggleMorePlatforms = function(button) {
            const hiddenSection = document.getElementById('hidden-platforms-badges');
            const showLessBtn = button.nextElementSibling;
            if (hiddenSection) {
                hiddenSection.style.display = 'block';
                button.style.display = 'none';
                if (showLessBtn) showLessBtn.style.display = 'inline-block';
            }
        };

        window.toggleLessPlatforms = function(button) {
            const hiddenSection = document.getElementById('hidden-platforms-badges');
            const showMoreBtn = button.previousElementSibling;
            if (hiddenSection) {
                hiddenSection.style.display = 'none';
                button.style.display = 'none';
                if (showMoreBtn) showMoreBtn.style.display = 'inline-block';
            }
        };

        // Genres
        window.toggleMoreGenres = function(button) {
            const hiddenSection = document.getElementById('hidden-genres-badges');
            const showLessBtn = button.nextElementSibling;
            if (hiddenSection) {
                hiddenSection.style.display = 'block';
                button.style.display = 'none';
                if (showLessBtn) showLessBtn.style.display = 'inline-block';
            }
        };

        window.toggleLessGenres = function(button) {
            const hiddenSection = document.getElementById('hidden-genres-badges');
            const showMoreBtn = button.previousElementSibling;
            if (hiddenSection) {
                hiddenSection.style.display = 'none';
                button.style.display = 'none';
                if (showMoreBtn) showMoreBtn.style.display = 'inline-block';
            }
        };

        // Keywords
        window.toggleMoreKeywords = function(button) {
            const hiddenSection = document.getElementById('hidden-keywords-badges');
            const showLessBtn = button.nextElementSibling;
            if (hiddenSection) {
                hiddenSection.style.display = 'block';
                button.style.display = 'none';
                if (showLessBtn) showLessBtn.style.display = 'inline-block';
            }
        };

        window.toggleLessKeywords = function(button) {
            const hiddenSection = document.getElementById('hidden-keywords-badges');
            const showMoreBtn = button.previousElementSibling;
            if (hiddenSection) {
                hiddenSection.style.display = 'none';
                button.style.display = 'none';
                if (showMoreBtn) showMoreBtn.style.display = 'inline-block';
            }
        };

        // Themes
        window.toggleMoreThemes = function(button) {
            const hiddenSection = document.getElementById('hidden-themes-badges');
            const showLessBtn = button.nextElementSibling;
            if (hiddenSection) {
                hiddenSection.style.display = 'block';
                button.style.display = 'none';
                if (showLessBtn) showLessBtn.style.display = 'inline-block';
            }
        };

        window.toggleLessThemes = function(button) {
            const hiddenSection = document.getElementById('hidden-themes-badges');
            const showMoreBtn = button.previousElementSibling;
            if (hiddenSection) {
                hiddenSection.style.display = 'none';
                button.style.display = 'none';
                if (showMoreBtn) showMoreBtn.style.display = 'inline-block';
            }
        };

        // Perspectives
        window.toggleMorePerspectives = function(button) {
            const hiddenSection = document.getElementById('hidden-perspectives-badges');
            const showLessBtn = button.nextElementSibling;
            if (hiddenSection) {
                hiddenSection.style.display = 'block';
                button.style.display = 'none';
                if (showLessBtn) showLessBtn.style.display = 'inline-block';
            }
        };

        window.toggleLessPerspectives = function(button) {
            const hiddenSection = document.getElementById('hidden-perspectives-badges');
            const showMoreBtn = button.previousElementSibling;
            if (hiddenSection) {
                hiddenSection.style.display = 'none';
                button.style.display = 'none';
                if (showMoreBtn) showMoreBtn.style.display = 'inline-block';
            }
        };

        // Game Modes
        window.toggleMoreGameModes = function(button) {
            const hiddenSection = document.getElementById('hidden-game-modes-badges');
            const showLessBtn = button.nextElementSibling;
            if (hiddenSection) {
                hiddenSection.style.display = 'block';
                button.style.display = 'none';
                if (showLessBtn) showLessBtn.style.display = 'inline-block';
            }
        };

        window.toggleLessGameModes = function(button) {
            const hiddenSection = document.getElementById('hidden-game-modes-badges');
            const showMoreBtn = button.previousElementSibling;
            if (hiddenSection) {
                hiddenSection.style.display = 'none';
                button.style.display = 'none';
                if (showMoreBtn) showMoreBtn.style.display = 'inline-block';
            }
        };
    }

    // ========== ИНИЦИАЛИЗАЦИЯ ВСЕГО ==========

    function initializeAll() {
        console.log('Initializing all functions...');

        // Запускаем все функции инициализации
        restoreSelectedCheckboxes();
        setupClearButtons();
        setupSearchFilters();
        setupShowAllToggles();
        setupAutoSubmit();
        setupActiveTagRemoval();
        setupCheckboxListeners();
        setupSectionToggles();
        setupShowMoreButtons();

        // Обновляем поля и сортируем списки
        setTimeout(() => {
            updateHiddenFields();
            sortFilterLists();
        }, 200);

        console.log('All functions initialized successfully');
    }

    // Запускаем инициализацию с небольшой задержкой
    setTimeout(initializeAll, 300);
});