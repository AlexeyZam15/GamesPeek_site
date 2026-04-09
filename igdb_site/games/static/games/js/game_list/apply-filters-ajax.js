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

        // CRITICAL: Check if we were in similar mode before ANY tab switching
        const wasSimilarMode = currentUrl.searchParams.get('find_similar') === '1' ||
                               currentUrl.searchParams.get('source_game');

        console.log('Was similar mode from URL:', wasSimilarMode);
        console.log('Active tab:', activeTab);

        // Collect search filters from search tab (always, regardless of active tab)
        const searchGenreCheckboxes = document.querySelectorAll('.search-genre-checkbox:checked');
        const searchGenreIds = Array.from(searchGenreCheckboxes).map(cb => cb.value);

        const searchPlatformCheckboxes = document.querySelectorAll('.search-platform-checkbox:checked');
        const searchPlatformIds = Array.from(searchPlatformCheckboxes).map(cb => cb.value);

        const searchKeywordCheckboxes = document.querySelectorAll('.search-keyword-checkbox:checked');
        const searchKeywordIds = Array.from(searchKeywordCheckboxes).map(cb => cb.value);

        const searchThemeCheckboxes = document.querySelectorAll('.search-theme-checkbox:checked');
        const searchThemeIds = Array.from(searchThemeCheckboxes).map(cb => cb.value);

        const searchPerspectiveCheckboxes = document.querySelectorAll('.search-perspective-checkbox:checked');
        const searchPerspectiveIds = Array.from(searchPerspectiveCheckboxes).map(cb => cb.value);

        const searchGameModeCheckboxes = document.querySelectorAll('.search-game-mode-checkbox:checked');
        const searchGameModeIds = Array.from(searchGameModeCheckboxes).map(cb => cb.value);

        const searchEngineCheckboxes = document.querySelectorAll('.search-engine-checkbox:checked');
        const searchEngineIds = Array.from(searchEngineCheckboxes).map(cb => cb.value);

        const searchGameTypeCheckboxes = document.querySelectorAll('.search-game-type-checkbox:checked');
        const searchGameTypeIds = Array.from(searchGameTypeCheckboxes).map(cb => cb.value);

        const searchYearStart = document.getElementById('search-manual-year-start')?.value;
        const searchYearEnd = document.getElementById('search-manual-year-end')?.value;

        // Collect similarity filters from similarity tab
        const similarityGenreCheckboxes = document.querySelectorAll('.genre-checkbox:checked');
        const similarityGenreIds = Array.from(similarityGenreCheckboxes).map(cb => cb.value);

        const similarityKeywordCheckboxes = document.querySelectorAll('.keyword-checkbox:checked');
        const similarityKeywordIds = Array.from(similarityKeywordCheckboxes).map(cb => cb.value);

        const similarityThemeCheckboxes = document.querySelectorAll('.theme-checkbox:checked');
        const similarityThemeIds = Array.from(similarityThemeCheckboxes).map(cb => cb.value);

        const similarityPerspectiveCheckboxes = document.querySelectorAll('.perspective-checkbox:checked');
        const similarityPerspectiveIds = Array.from(similarityPerspectiveCheckboxes).map(cb => cb.value);

        const similarityGameModeCheckboxes = document.querySelectorAll('.game-mode-checkbox:checked');
        const similarityGameModeIds = Array.from(similarityGameModeCheckboxes).map(cb => cb.value);

        const similarityEngineCheckboxes = document.querySelectorAll('.engine-checkbox:checked');
        const similarityEngineIds = Array.from(similarityEngineCheckboxes).map(cb => cb.value);

        const similarityPlatformCheckboxes = document.querySelectorAll('.platform-checkbox:checked');
        const similarityPlatformIds = Array.from(similarityPlatformCheckboxes).map(cb => cb.value);

        const similarityGameTypeCheckboxes = document.querySelectorAll('.game-type-checkbox:checked');
        const similarityGameTypeIds = Array.from(similarityGameTypeCheckboxes).map(cb => cb.value);

        const similarityYearStart = document.getElementById('manual-year-start')?.value;
        const similarityYearEnd = document.getElementById('manual-year-end')?.value;

        // Determine if we should stay in similarity mode
        // Stay in similarity mode if: we were in similarity mode OR similarity filters are selected
        const hasSimilarityFilters = similarityGenreIds.length > 0 ||
                                      similarityKeywordIds.length > 0 ||
                                      similarityThemeIds.length > 0 ||
                                      similarityPerspectiveIds.length > 0 ||
                                      similarityGameModeIds.length > 0 ||
                                      similarityEngineIds.length > 0 ||
                                      similarityPlatformIds.length > 0 ||
                                      similarityGameTypeIds.length > 0;

        const stayInSimilarityMode = wasSimilarMode || hasSimilarityFilters;

        console.log('Stay in similarity mode:', stayInSimilarityMode);
        console.log('Has similarity filters:', hasSimilarityFilters);

        if (stayInSimilarityMode) {
            // ===== SIMILARITY MODE WITH SEARCH FILTERS =====
            console.log('Similarity mode: keeping similarity and adding search filters as additional filtering');

            // Enable similarity mode
            newUrl.searchParams.set('find_similar', '1');

            // Preserve source_game if exists
            const sourceGameId = currentUrl.searchParams.get('source_game');
            if (sourceGameId) {
                newUrl.searchParams.set('source_game', sourceGameId);
            }

            // Add similarity filters (from similarity tab)
            if (similarityGenreIds.length > 0) {
                newUrl.searchParams.set('g', similarityGenreIds.join(','));
            }

            if (similarityKeywordIds.length > 0) {
                newUrl.searchParams.set('k', similarityKeywordIds.join(','));
            }

            if (similarityThemeIds.length > 0) {
                newUrl.searchParams.set('t', similarityThemeIds.join(','));
            }

            if (similarityPerspectiveIds.length > 0) {
                newUrl.searchParams.set('pp', similarityPerspectiveIds.join(','));
            }

            if (similarityGameModeIds.length > 0) {
                newUrl.searchParams.set('gm', similarityGameModeIds.join(','));
            }

            if (similarityEngineIds.length > 0) {
                newUrl.searchParams.set('e', similarityEngineIds.join(','));
            }

            if (similarityPlatformIds.length > 0) {
                newUrl.searchParams.set('p', similarityPlatformIds.join(','));
            }

            if (similarityGameTypeIds.length > 0) {
                newUrl.searchParams.set('gt', similarityGameTypeIds.join(','));
            }

            if (similarityYearStart && similarityYearStart.trim() !== '') {
                newUrl.searchParams.set('ys', similarityYearStart);
            }
            if (similarityYearEnd && similarityYearEnd.trim() !== '') {
                newUrl.searchParams.set('ye', similarityYearEnd);
            }

            // ADD SEARCH FILTERS as additional filtering (search_* parameters)
            if (searchGenreIds.length > 0) {
                newUrl.searchParams.set('search_g', searchGenreIds.join(','));
                console.log('Adding search_g to similarity mode:', searchGenreIds.join(','));
            }

            if (searchPlatformIds.length > 0) {
                newUrl.searchParams.set('search_p', searchPlatformIds.join(','));
                console.log('Adding search_p to similarity mode:', searchPlatformIds.join(','));
            }

            if (searchKeywordIds.length > 0) {
                newUrl.searchParams.set('search_k', searchKeywordIds.join(','));
                console.log('Adding search_k to similarity mode:', searchKeywordIds.join(','));
            }

            if (searchThemeIds.length > 0) {
                newUrl.searchParams.set('search_t', searchThemeIds.join(','));
                console.log('Adding search_t to similarity mode:', searchThemeIds.join(','));
            }

            if (searchPerspectiveIds.length > 0) {
                newUrl.searchParams.set('search_pp', searchPerspectiveIds.join(','));
                console.log('Adding search_pp to similarity mode:', searchPerspectiveIds.join(','));
            }

            if (searchGameModeIds.length > 0) {
                newUrl.searchParams.set('search_gm', searchGameModeIds.join(','));
                console.log('Adding search_gm to similarity mode:', searchGameModeIds.join(','));
            }

            if (searchEngineIds.length > 0) {
                newUrl.searchParams.set('search_e', searchEngineIds.join(','));
                console.log('Adding search_e to similarity mode:', searchEngineIds.join(','));
            }

            if (searchGameTypeIds.length > 0) {
                newUrl.searchParams.set('search_gt', searchGameTypeIds.join(','));
                console.log('Adding search_gt to similarity mode:', searchGameTypeIds.join(','));
            }

            if (searchYearStart && searchYearStart.trim() !== '') {
                newUrl.searchParams.set('search_ys', searchYearStart);
                console.log('Adding search_ys to similarity mode:', searchYearStart);
            }
            if (searchYearEnd && searchYearEnd.trim() !== '') {
                newUrl.searchParams.set('search_ye', searchYearEnd);
                console.log('Adding search_ye to similarity mode:', searchYearEnd);
            }

        } else {
            // ===== PURE SEARCH MODE (no similarity) =====
            console.log('Pure search mode: disabling similarity');

            // Remove all similarity parameters
            newUrl.searchParams.delete('find_similar');
            newUrl.searchParams.delete('source_game');
            newUrl.searchParams.delete('g');
            newUrl.searchParams.delete('k');
            newUrl.searchParams.delete('t');
            newUrl.searchParams.delete('pp');
            newUrl.searchParams.delete('gm');
            newUrl.searchParams.delete('e');
            newUrl.searchParams.delete('p');
            newUrl.searchParams.delete('gt');
            newUrl.searchParams.delete('ys');
            newUrl.searchParams.delete('ye');

            // Add search filters
            if (searchGenreIds.length > 0) {
                newUrl.searchParams.set('search_g', searchGenreIds.join(','));
            }

            if (searchPlatformIds.length > 0) {
                newUrl.searchParams.set('search_p', searchPlatformIds.join(','));
            }

            if (searchKeywordIds.length > 0) {
                newUrl.searchParams.set('search_k', searchKeywordIds.join(','));
            }

            if (searchThemeIds.length > 0) {
                newUrl.searchParams.set('search_t', searchThemeIds.join(','));
            }

            if (searchPerspectiveIds.length > 0) {
                newUrl.searchParams.set('search_pp', searchPerspectiveIds.join(','));
            }

            if (searchGameModeIds.length > 0) {
                newUrl.searchParams.set('search_gm', searchGameModeIds.join(','));
            }

            if (searchEngineIds.length > 0) {
                newUrl.searchParams.set('search_e', searchEngineIds.join(','));
            }

            if (searchGameTypeIds.length > 0) {
                newUrl.searchParams.set('search_gt', searchGameTypeIds.join(','));
            }

            if (searchYearStart && searchYearStart.trim() !== '') {
                newUrl.searchParams.set('search_ys', searchYearStart);
            }
            if (searchYearEnd && searchYearEnd.trim() !== '') {
                newUrl.searchParams.set('search_ye', searchYearEnd);
            }
        }

        // Set page=1 and sorting
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