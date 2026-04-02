// games/static/games/js/game_list/apply-filters-ajax.js

(function() {
    console.log('Apply Filters AJAX: initializing');

    // Ждем загрузки DOM
    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', init);
    } else {
        init();
    }

    function init() {
        console.log('Apply Filters AJAX: setting up');

        // Находим кнопку Apply Filters по тексту или классу
        const applyButton = findApplyButton();
        if (!applyButton) {
            console.log('Apply Filters AJAX: button not found');
            return;
        }

        console.log('Apply Filters AJAX: button found', applyButton);

        // Создаем контейнер для результатов если его нет
        ensureResultsContainer();

        // Удаляем старые обработчики
        const newButton = applyButton.cloneNode(true);
        applyButton.parentNode.replaceChild(newButton, applyButton);

        // Добавляем новый обработчик ТОЛЬКО на клик по кнопке
        newButton.addEventListener('click', handleApplyFilters);

        // ВНИМАНИЕ: Удален блок с перехватом отправки формы!
        // Форма теперь отправляется нормально для поиска из навигации
    }

    function findApplyButton() {
        // Ищем по тексту
        const buttons = document.querySelectorAll('button');
        for (let button of buttons) {
            if (button.textContent.includes('Apply Filters')) {
                return button;
            }
        }

        // Ищем по классу
        const applyMain = document.querySelector('.apply-filters-main');
        if (applyMain) return applyMain;

        // Ищем по типу submit
        return document.querySelector('button[type="submit"]');
    }

    function ensureResultsContainer() {
        let resultsContainer = document.getElementById('games-results-container');
        if (!resultsContainer) {
            resultsContainer = document.createElement('div');
            resultsContainer.id = 'games-results-container';

            const gamesContainer = document.querySelector('.games-container');
            if (gamesContainer && gamesContainer.parentNode) {
                gamesContainer.parentNode.insertBefore(resultsContainer, gamesContainer);
            }
        }
    }

    function handleApplyFilters(e) {
        e.preventDefault();
        e.stopPropagation();

        console.log('Apply Filters AJAX: clicked');

        const currentUrl = new URL(window.location.href);
        const activeTab = document.querySelector('.filter-tab-link.active')?.dataset.tab;
        console.log('Active tab:', activeTab);

        const newUrl = new URL('/games/', window.location.origin);

        if (activeTab === 'search-filters-pane') {
            // ===== SEARCH MODE - НЕ ДОБАВЛЯЕМ find_similar =====
            // Явно удаляем find_similar
            newUrl.searchParams.delete('find_similar');
            newUrl.searchParams.delete('source_game');

            // Удаляем все similarity параметры (g, k, t, pp, gm, e)
            newUrl.searchParams.delete('g');
            newUrl.searchParams.delete('k');
            newUrl.searchParams.delete('t');
            newUrl.searchParams.delete('pp');
            newUrl.searchParams.delete('gm');
            newUrl.searchParams.delete('e');

            // Собираем search параметры
            const searchGenreCheckboxes = document.querySelectorAll('.search-genre-checkbox:checked');
            const searchGenreIds = Array.from(searchGenreCheckboxes).map(cb => cb.value);

            if (searchGenreIds.length > 0) {
                newUrl.searchParams.set('search_g', searchGenreIds.join(','));
            }

            const platformCheckboxes = document.querySelectorAll('.search-platform-checkbox:checked');
            const platformIds = Array.from(platformCheckboxes).map(cb => cb.value);
            if (platformIds.length > 0) {
                newUrl.searchParams.set('search_p', platformIds.join(','));
            }

            const searchKeywordCheckboxes = document.querySelectorAll('.search-keyword-checkbox:checked');
            const searchKeywordIds = Array.from(searchKeywordCheckboxes).map(cb => cb.value);
            if (searchKeywordIds.length > 0) {
                newUrl.searchParams.set('search_k', searchKeywordIds.join(','));
            }

            const searchThemeCheckboxes = document.querySelectorAll('.search-theme-checkbox:checked');
            const searchThemeIds = Array.from(searchThemeCheckboxes).map(cb => cb.value);
            if (searchThemeIds.length > 0) {
                newUrl.searchParams.set('search_t', searchThemeIds.join(','));
            }

            const searchPerspectiveCheckboxes = document.querySelectorAll('.search-perspective-checkbox:checked');
            const searchPerspectiveIds = Array.from(searchPerspectiveCheckboxes).map(cb => cb.value);
            if (searchPerspectiveIds.length > 0) {
                newUrl.searchParams.set('search_pp', searchPerspectiveIds.join(','));
            }

            const searchGameModeCheckboxes = document.querySelectorAll('.search-game-mode-checkbox:checked');
            const searchGameModeIds = Array.from(searchGameModeCheckboxes).map(cb => cb.value);
            if (searchGameModeIds.length > 0) {
                newUrl.searchParams.set('search_gm', searchGameModeIds.join(','));
            }

            const searchEngineCheckboxes = document.querySelectorAll('.search-engine-checkbox:checked');
            const searchEngineIds = Array.from(searchEngineCheckboxes).map(cb => cb.value);
            if (searchEngineIds.length > 0) {
                newUrl.searchParams.set('search_e', searchEngineIds.join(','));
            }

            const gameTypeCheckboxes = document.querySelectorAll('.search-game-type-checkbox:checked');
            const gameTypeIds = Array.from(gameTypeCheckboxes).map(cb => cb.value);
            if (gameTypeIds.length > 0) {
                newUrl.searchParams.set('search_gt', gameTypeIds.join(','));
            }

            const yearStart = document.getElementById('search-manual-year-start')?.value;
            const yearEnd = document.getElementById('search-manual-year-end')?.value;

            if (yearStart && yearStart.trim() !== '') {
                newUrl.searchParams.set('search_ys', yearStart);
            }
            if (yearEnd && yearEnd.trim() !== '') {
                newUrl.searchParams.set('search_ye', yearEnd);
            }

        } else {
            // ===== SIMILARITY MODE =====
            newUrl.searchParams.set('find_similar', '1');

            const genreCheckboxes = document.querySelectorAll('.genre-checkbox:checked');
            const genreIds = Array.from(genreCheckboxes).map(cb => cb.value);
            if (genreIds.length > 0) {
                newUrl.searchParams.set('g', genreIds.join(','));
            }

            const keywordCheckboxes = document.querySelectorAll('.keyword-checkbox:checked');
            const keywordIds = Array.from(keywordCheckboxes).map(cb => cb.value);
            if (keywordIds.length > 0) {
                newUrl.searchParams.set('k', keywordIds.join(','));
            }

            const themeCheckboxes = document.querySelectorAll('.theme-checkbox:checked');
            const themeIds = Array.from(themeCheckboxes).map(cb => cb.value);
            if (themeIds.length > 0) {
                newUrl.searchParams.set('t', themeIds.join(','));
            }

            const perspectiveCheckboxes = document.querySelectorAll('.perspective-checkbox:checked');
            const perspectiveIds = Array.from(perspectiveCheckboxes).map(cb => cb.value);
            if (perspectiveIds.length > 0) {
                newUrl.searchParams.set('pp', perspectiveIds.join(','));
            }

            const gameModeCheckboxes = document.querySelectorAll('.game-mode-checkbox:checked');
            const gameModeIds = Array.from(gameModeCheckboxes).map(cb => cb.value);
            if (gameModeIds.length > 0) {
                newUrl.searchParams.set('gm', gameModeIds.join(','));
            }

            const engineCheckboxes = document.querySelectorAll('.engine-checkbox:checked');
            const engineIds = Array.from(engineCheckboxes).map(cb => cb.value);
            if (engineIds.length > 0) {
                newUrl.searchParams.set('e', engineIds.join(','));
            }
        }

        // Устанавливаем page=1 и сортировку
        newUrl.searchParams.set('page', '1');

        const sortSelect = document.querySelector('select[name="sort"]');
        if (sortSelect && sortSelect.value) {
            newUrl.searchParams.set('sort', sortSelect.value);
        }

        console.log('Final URL:', newUrl.toString());
        loadGamesPage(newUrl.toString());
    }

    function loadGamesPage(url) {
        console.log('Loading games via AJAX:', url);

        // Показываем загрузку
        showLoading();

        // Используем AJAX endpoint
        const ajaxUrl = '/ajax/load-games-page/' + url.substring(url.indexOf('?'));

        console.log('AJAX request URL:', ajaxUrl);

        fetch(ajaxUrl, {
            headers: {
                'X-Requested-With': 'XMLHttpRequest',
                'Accept': 'text/html'
            },
            credentials: 'same-origin'
        })
        .then(response => {
            console.log('AJAX response status:', response.status);
            console.log('AJAX response headers:', {
                page: response.headers.get('X-AJAX-Page'),
                total_pages: response.headers.get('X-Total-Pages')
            });
            if (!response.ok) throw new Error(`HTTP error ${response.status}`);
            return response.text();
        })
        .then(html => {
            console.log('AJAX response received, length:', html.length);

            // Обновляем контейнер с результатами
            const resultsContainer = document.getElementById('games-results-container');
            if (resultsContainer) {
                resultsContainer.innerHTML = html;
                console.log('Results container updated');
            } else {
                console.log('Results container not found');
                // Если нет контейнера, создаем
                const newContainer = document.createElement('div');
                newContainer.id = 'games-results-container';
                newContainer.innerHTML = html;

                const gamesContainer = document.querySelector('.games-container');
                if (gamesContainer && gamesContainer.parentNode) {
                    gamesContainer.parentNode.insertBefore(newContainer, gamesContainer);
                    gamesContainer.style.display = 'none';
                    console.log('Created new results container');
                }
            }

            // Обновляем URL в адресной строке БЕЗ перезагрузки
            window.history.pushState({ path: url }, '', url);
            console.log('URL updated to:', url);

            // Скрываем загрузку
            hideLoading();

            // Переинициализируем компоненты
            setTimeout(reinitializeComponents, 100);

            console.log('Games loaded successfully');
        })
        .catch(error => {
            console.error('Error loading games:', error);
            hideLoading();

            // Если AJAX не работает, делаем обычный редирект
            console.log('AJAX failed, redirecting to:', url);
            window.location.href = url;
        });
    }

    function showLoading() {
        const gamesContainer = document.querySelector('.games-container');
        if (gamesContainer) {
            gamesContainer.classList.add('loading-visible');
        }
    }

    function hideLoading() {
        const gamesContainer = document.querySelector('.games-container');
        if (gamesContainer) {
            gamesContainer.classList.remove('loading-visible');
        }
    }

    function reinitializeComponents() {
        console.log('Reinitializing components');

        // Перезапускаем similarity display
        if (window.SimilarityDisplay && typeof window.SimilarityDisplay.refresh === 'function') {
            window.SimilarityDisplay.refresh();
        }

        // Перезапускаем similarity compare
        if (window.SimilarityCompare && typeof window.SimilarityCompare.refresh === 'function') {
            window.SimilarityCompare.refresh();
        }

        // Вызываем событие
        document.dispatchEvent(new CustomEvent('games-grid-updated'));
    }

})();