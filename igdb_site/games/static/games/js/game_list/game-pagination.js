// games/static/games/js/game_list/game-pagination.js

const GamePagination = {
    // Конфигурация
    config: {
        itemsPerPage: 16,
        currentPage: 1,
        itemsSelector: '.game-card-container',
        containerSelector: '.games-container',
        // Элементы для пагинации вверху
        paginationTopSelector: '.games-pagination-top',
        prevButtonTopId: '#games-prev-top',
        nextButtonTopId: '#games-next-top',
        startElementTopId: '#games-start-top',
        endElementTopId: '#games-end-top',
        currentElementTopId: '#games-current-top',
        totalElementTopId: '#games-total-all-top',
        totalPagesElementTopId: '#games-total-pages-top',
        pageNumbersContainerTopId: '#games-page-numbers-top',
        // Элементы для пагинации внизу
        paginationBottomSelector: '.games-pagination-bottom',
        prevButtonBottomId: '#games-prev',
        nextButtonBottomId: '#games-next',
        startElementBottomId: '#games-start',
        endElementBottomId: '#games-end',
        currentElementBottomId: '#games-current',
        totalElementBottomId: '#games-total-all',
        totalPagesElementBottomId: '#games-total-pages',
        pageNumbersContainerBottomId: '#games-page-numbers',
        maxVisiblePages: 7
    },

    // Состояние
    state: {
        totalItems: 0,
        totalPages: 0,
        loadedPages: new Set(),
        isLoading: false,
        gameItems: [],
    },

    // Инициализация
    init() {
        console.log('Initializing lazy games pagination...');

        // Первая страница уже загружена с сервера
        this.state.loadedPages.add(1);

        // Получаем информацию о пагинации из DOM
        this.loadPaginationInfoFromDOM();

        // Получаем текущие игры с первой страницы
        this.loadCurrentGamesFromDOM();

        // Всегда показываем пагинацию, даже если totalPages = 1
        // (она может стать больше при применении фильтров)
        this.createPageNumbersContainer('top');
        this.createPageNumbersContainer('bottom');

        this.setupPagination();
        this.showPage(1, false);

        console.log(`Lazy pagination initialized: ${this.state.totalItems} items, ${this.state.totalPages} pages`);
    },

    // Загрузка информации о пагинации из DOM
    loadPaginationInfoFromDOM() {
        const totalElement = document.querySelector(this.config.totalElementBottomId);
        const totalPagesElement = document.querySelector(this.config.totalPagesElementBottomId);
        const startElement = document.querySelector(this.config.startElementBottomId);
        const endElement = document.querySelector(this.config.endElementBottomId);

        if (totalElement) {
            this.state.totalItems = parseInt(totalElement.textContent) || 0;
        } else {
            // Если элемента нет, пытаемся получить из другого места
            const totalTopElement = document.querySelector(this.config.totalElementTopId);
            if (totalTopElement) {
                this.state.totalItems = parseInt(totalTopElement.textContent) || 0;
            }
        }

        if (totalPagesElement) {
            const totalPagesText = totalPagesElement.textContent;
            if (totalPagesText && totalPagesText.trim() !== '') {
                this.state.totalPages = parseInt(totalPagesText) || 1;
            } else {
                // Если элемент пустой, вычисляем из totalItems
                this.state.totalPages = Math.ceil(this.state.totalItems / this.config.itemsPerPage);
            }
        } else {
            // Если элемента нет, вычисляем из totalItems
            this.state.totalPages = Math.ceil(this.state.totalItems / this.config.itemsPerPage);
        }

        // Дополнительная проверка: если totalItems > 0, но totalPages = 1, пересчитываем
        if (this.state.totalItems > this.config.itemsPerPage && this.state.totalPages <= 1) {
            this.state.totalPages = Math.ceil(this.state.totalItems / this.config.itemsPerPage);
            console.log(`Recalculated totalPages: ${this.state.totalPages} from ${this.state.totalItems} items`);
        }

        console.log(`Pagination info: ${this.state.totalItems} items, ${this.state.totalPages} pages`);

        // Всегда показываем пагинацию, даже если только одна страница
        this.showPagination();
    },

    // Загрузить текущие игры из DOM (первая страница)
    loadCurrentGamesFromDOM() {
        const container = document.querySelector(this.config.containerSelector);
        if (!container) return;

        // Находим row внутри контейнера
        const rowElement = container.querySelector('.row');
        if (!rowElement) {
            console.error('Row element not found in games container');
            return;
        }

        // Получаем все текущие игры
        const gameElements = rowElement.querySelectorAll('.game-card-container');

        // Сохраняем игры для первой страницы
        gameElements.forEach((element, index) => {
            this.state.gameItems[index] = element;
        });

        console.log(`Loaded ${gameElements.length} games from current page`);
    },

    // Создаем контейнер для номеров страниц
    createPageNumbersContainer(position) {
        const containerId = position === 'top' ?
            this.config.pageNumbersContainerTopId :
            this.config.pageNumbersContainerBottomId;

        const buttonId = position === 'top' ?
            this.config.prevButtonTopId :
            this.config.prevButtonBottomId;

        if (document.querySelector(containerId)) {
            return;
        }

        const paginationContainer = document.querySelector(
            position === 'top' ?
            this.config.paginationTopSelector :
            this.config.paginationBottomSelector
        );
        if (!paginationContainer) return;

        const prevButton = document.querySelector(buttonId);
        if (!prevButton) return;

        const pageNumbersContainer = document.createElement('div');
        pageNumbersContainer.id = containerId.substring(1);
        pageNumbersContainer.className = 'page-numbers-container';
        pageNumbersContainer.style.minWidth = '200px';

        prevButton.parentNode.insertBefore(pageNumbersContainer, prevButton.nextSibling);
    },

    // Настройка пагинации
    setupPagination() {
        this.updatePageNumbers();
        this.setupNavigationButtons();
        this.updatePageInfo();
        this.updateNavigationButtons();
    },

    // Обновить номера страниц
    updatePageNumbers() {
        this.updateSinglePageNumbers('top');
        this.updateSinglePageNumbers('bottom');
    },

    // Обновить номера страниц для одной пагинации
    updateSinglePageNumbers(position) {
        const containerId = position === 'top' ?
            this.config.pageNumbersContainerTopId :
            this.config.pageNumbersContainerBottomId;

        const pageNumbersContainer = document.querySelector(containerId);
        if (!pageNumbersContainer) return;

        pageNumbersContainer.innerHTML = '';

        // Если всего 1 страница, показываем только её
        if (this.state.totalPages <= 1) {
            this.createPageNumberButton(pageNumbersContainer, 1, position);
            return;
        }

        const { startPage, endPage } = this.getVisiblePageRange(this.config.currentPage);

        if (startPage > 1) {
            this.createPageNumberButton(pageNumbersContainer, 1, position);

            if (startPage > 2) {
                this.createEllipsis(pageNumbersContainer);
            }
        }

        for (let i = startPage; i <= endPage; i++) {
            this.createPageNumberButton(pageNumbersContainer, i, position);
        }

        if (endPage < this.state.totalPages) {
            if (endPage < this.state.totalPages - 1) {
                this.createEllipsis(pageNumbersContainer);
            }

            this.createPageNumberButton(pageNumbersContainer, this.state.totalPages, position);
        }
    },

    // Создать кнопку номера страницы
    createPageNumberButton(container, pageNumber, position) {
        const button = document.createElement('button');
        button.type = 'button';
        button.className = 'btn btn-sm page-number-btn';

        if (pageNumber === this.config.currentPage) {
            button.classList.add('btn-primary');
            button.title = 'Текущая страница';
        } else if (this.state.loadedPages.has(pageNumber)) {
            button.classList.add('btn-outline-primary');
            button.title = 'Загружена (кликните для показа)';
        } else {
            button.classList.add('btn-outline-secondary');
            button.title = 'Еще не загружена (кликните для загрузки)';
        }

        button.textContent = pageNumber;
        button.dataset.page = pageNumber;
        button.dataset.position = position;

        button.addEventListener('click', (e) => {
            e.preventDefault();
            e.stopPropagation(); // Предотвращаем всплытие

            if (!this.state.loadedPages.has(pageNumber)) {
                this.forceLoadPage(pageNumber);
            } else {
                this.showPage(pageNumber, false);
            }
        });

        container.appendChild(button);
        return button;
    },

    // Принудительно загрузить страницу
    forceLoadPage(pageNumber) {
        console.log(`Force loading page ${pageNumber}...`);

        this.updatePageButtonState(pageNumber, 'loading');

        this.loadPageFromServer(pageNumber)
            .then(games => {
                this.processPageGames(pageNumber, games);
                this.showPage(pageNumber, false);
                console.log(`Page ${pageNumber} force loaded`);
            })
            .catch(error => {
                console.error(`Error force loading page ${pageNumber}:`, error);
                this.updatePageButtonState(pageNumber, 'error');
            });
    },

    // Загрузить страницу с сервера
    loadPageFromServer(pageNumber) {
        return new Promise((resolve, reject) => {
            const url = new URL(window.location.href);
            url.searchParams.set('page', pageNumber);
            url.searchParams.set('_ajax', '1'); // Флаг AJAX запроса

            fetch(url.toString())
                .then(response => {
                    if (!response.ok) {
                        throw new Error(`HTTP error! status: ${response.status}`);
                    }
                    return response.text();
                })
                .then(html => {
                    const parser = new DOMParser();
                    const doc = parser.parseFromString(html, 'text/html');

                    // Извлекаем игры из row элемента
                    const gamesContainer = doc.querySelector(this.config.containerSelector);
                    const rowElement = gamesContainer ? gamesContainer.querySelector('.row') : null;

                    if (rowElement) {
                        const gameElements = rowElement.querySelectorAll('.game-card-container');
                        const games = Array.from(gameElements).map(el => el.outerHTML);

                        // Сохраняем в кэш
                        this.savePageToStorage(pageNumber, games);

                        resolve(games);
                    } else {
                        reject(new Error('Row element not found in response'));
                    }
                })
                .catch(error => {
                    reject(error);
                });
        });
    },

    // Обработать игры страницы
    processPageGames(pageNumber, games) {
        // Сохраняем игры в состоянии
        const startIndex = (pageNumber - 1) * this.config.itemsPerPage;

        games.forEach((gameHtml, index) => {
            const gameIndex = startIndex + index;

            // Создаем элемент из HTML
            const tempDiv = document.createElement('div');
            tempDiv.innerHTML = gameHtml;
            const gameElement = tempDiv.firstChild;

            if (gameElement) {
                gameElement.style.display = 'none';
                gameElement.dataset.page = pageNumber;

                // Добавляем в массив игр
                if (!this.state.gameItems[gameIndex]) {
                    this.state.gameItems[gameIndex] = gameElement;
                }
            }
        });

        this.state.loadedPages.add(pageNumber);
        this.updatePageButtonState(pageNumber, 'loaded');
        console.log(`Page ${pageNumber} processed (${games.length} games)`);
    },

    // Сохранить страницу в хранилище
    savePageToStorage(pageNumber, games) {
        try {
            const key = `page_${pageNumber}_games`;
            sessionStorage.setItem(key, JSON.stringify(games));
        } catch (error) {
            console.error(`Error saving page ${pageNumber} to storage:`, error);
        }
    },

    // Получить страницу из хранилища
    getPageFromStorage(pageNumber) {
        try {
            const key = `page_${pageNumber}_games`;
            const data = sessionStorage.getItem(key);
            if (data) {
                return JSON.parse(data);
            }
        } catch (error) {
            console.error(`Error reading page ${pageNumber} from storage:`, error);
        }
        return null;
    },

    // Создать элемент многоточия
    createEllipsis(container) {
        const span = document.createElement('span');
        span.className = 'text-muted mx-1';
        span.textContent = '...';
        container.appendChild(span);
    },

    // Получить диапазон видимых страниц
    getVisiblePageRange(currentPage) {
        if (this.state.totalPages <= this.config.maxVisiblePages) {
            return { startPage: 1, endPage: this.state.totalPages };
        }

        let startPage = currentPage - Math.floor(this.config.maxVisiblePages / 2);
        let endPage = currentPage + Math.floor(this.config.maxVisiblePages / 2);

        if (startPage < 1) {
            startPage = 1;
            endPage = this.config.maxVisiblePages;
        }

        if (endPage > this.state.totalPages) {
            endPage = this.state.totalPages;
            startPage = endPage - this.config.maxVisiblePages + 1;
        }

        return { startPage, endPage };
    },

    // Настройка кнопок навигации
    setupNavigationButtons() {
        this.setupNavigationButtonPair('top');
        this.setupNavigationButtonPair('bottom');
    },

    // Настройка пары кнопок (prev/next)
    setupNavigationButtonPair(position) {
        const prevBtnId = position === 'top' ? this.config.prevButtonTopId : this.config.prevButtonBottomId;
        const nextBtnId = position === 'top' ? this.config.nextButtonTopId : this.config.nextButtonBottomId;

        const prevBtn = document.querySelector(prevBtnId);
        const nextBtn = document.querySelector(nextBtnId);

        if (prevBtn) {
            const newPrevBtn = prevBtn.cloneNode(true);
            prevBtn.parentNode.replaceChild(newPrevBtn, prevBtn);

            newPrevBtn.addEventListener('click', (e) => {
                e.preventDefault();
                e.stopPropagation(); // Предотвращаем всплытие
                if (this.config.currentPage > 1) {
                    const prevPage = this.config.currentPage - 1;
                    if (!this.state.loadedPages.has(prevPage)) {
                        this.forceLoadPage(prevPage);
                    } else {
                        this.showPage(prevPage, false);
                    }
                }
            });
        }

        if (nextBtn) {
            const newNextBtn = nextBtn.cloneNode(true);
            nextBtn.parentNode.replaceChild(newNextBtn, nextBtn);

            newNextBtn.addEventListener('click', (e) => {
                e.preventDefault();
                e.stopPropagation(); // Предотвращаем всплытие
                if (this.config.currentPage < this.state.totalPages) {
                    const nextPage = this.config.currentPage + 1;
                    if (!this.state.loadedPages.has(nextPage)) {
                        this.forceLoadPage(nextPage);
                    } else {
                        this.showPage(nextPage, false);
                    }
                }
            });
        }
    },

    // Показать определенную страницу
    showPage(pageNumber, isBackground = false) {
        if (pageNumber < 1 || pageNumber > this.state.totalPages) {
            return;
        }

        // Если страница еще не загружена, загружаем ее
        if (!this.state.loadedPages.has(pageNumber)) {
            console.log(`Page ${pageNumber} not loaded, loading now...`);
            this.forceLoadPage(pageNumber);
            return;
        }

        this._showPageAfterLoad(pageNumber);
    },

    // Внутренний метод для отображения страницы после загрузки
    _showPageAfterLoad(pageNumber) {
        this.config.currentPage = pageNumber;

        const startIndex = (pageNumber - 1) * this.config.itemsPerPage;
        const endIndex = Math.min(startIndex + this.config.itemsPerPage, this.state.totalItems);

        console.log(`Showing page ${pageNumber}: items ${startIndex + 1}-${endIndex}`);

        // Получаем контейнер
        const container = document.querySelector(this.config.containerSelector);
        if (!container) {
            console.error('Games container not found');
            return;
        }

        // Находим или создаем row элемент
        let rowElement = container.querySelector('.row');
        if (!rowElement) {
            rowElement = document.createElement('div');
            rowElement.className = 'row';
            container.appendChild(rowElement);
        }

        // Очищаем row элемент
        rowElement.innerHTML = '';

        // Добавляем игры для текущей страницы в row
        for (let i = startIndex; i < endIndex; i++) {
            if (this.state.gameItems[i]) {
                const gameElement = this.state.gameItems[i];
                gameElement.style.display = 'block';
                gameElement.style.animation = 'pageFadeIn 0.3s ease';
                rowElement.appendChild(gameElement);
            } else {
                // Если игры нет в кэше, создаем placeholder
                const placeholder = this.createGamePlaceholder(i + 1);
                rowElement.appendChild(placeholder);
            }
        }

        // Обновляем UI
        this.updatePageNumbers();
        this.updatePageInfo();
        this.updateNavigationButtons();

        // Обновляем URL без перезагрузки страницы
        this.updateBrowserUrl(pageNumber);

        // УБРАЛИ вызов scrollToGamesContainer - не прокручиваем страницу
        // this.scrollToGamesContainer();
    },

    // Создать placeholder для игры
    createGamePlaceholder(index) {
        const placeholder = document.createElement('div');
        placeholder.className = 'col-xl-3 col-lg-4 col-md-6 mb-4 game-card-container';
        placeholder.innerHTML = `
            <div class="game-card placeholder">
                <div class="card h-100">
                    <div class="card-body text-center">
                        <div class="spinner-border text-primary mb-3" role="status">
                            <span class="visually-hidden">Loading...</span>
                        </div>
                        <p class="text-muted">Loading game ${index}...</p>
                    </div>
                </div>
            </div>
        `;
        return placeholder;
    },

    // Обновить URL браузера без перезагрузки
    updateBrowserUrl(pageNumber) {
        const url = new URL(window.location.href);
        url.searchParams.set('page', pageNumber);

        // Сохраняем в истории без перезагрузки
        window.history.replaceState({}, document.title, url.toString());
    },

    // Обновить информацию о странице
    updatePageInfo() {
        const startIndex = (this.config.currentPage - 1) * this.config.itemsPerPage + 1;
        const endIndex = Math.min(this.config.currentPage * this.config.itemsPerPage, this.state.totalItems);

        this.updateSinglePageInfo('top', startIndex, endIndex);
        this.updateSinglePageInfo('bottom', startIndex, endIndex);
    },

    updateSinglePageInfo(position, startIndex, endIndex) {
        const startElement = document.querySelector(
            position === 'top' ? this.config.startElementTopId : this.config.startElementBottomId
        );
        const endElement = document.querySelector(
            position === 'top' ? this.config.endElementTopId : this.config.endElementBottomId
        );
        const currentElement = document.querySelector(
            position === 'top' ? this.config.currentElementTopId : this.config.currentElementBottomId
        );
        const totalElement = document.querySelector(
            position === 'top' ? this.config.totalElementTopId : this.config.totalElementBottomId
        );
        const totalPagesElement = document.querySelector(
            position === 'top' ? this.config.totalPagesElementTopId : this.config.totalPagesElementBottomId
        );

        if (startElement) startElement.textContent = startIndex;
        if (endElement) endElement.textContent = endIndex;
        if (currentElement) currentElement.textContent = this.config.currentPage;
        if (totalElement) totalElement.textContent = this.state.totalItems;
        if (totalPagesElement) totalPagesElement.textContent = this.state.totalPages;
    },

    // Обновить кнопки навигации
    updateNavigationButtons() {
        this.updateNavigationButtonPair('top');
        this.updateNavigationButtonPair('bottom');
    },

    updateNavigationButtonPair(position) {
        const prevBtnId = position === 'top' ? this.config.prevButtonTopId : this.config.prevButtonBottomId;
        const nextBtnId = position === 'top' ? this.config.nextButtonTopId : this.config.nextButtonBottomId;

        const prevBtn = document.querySelector(prevBtnId);
        const nextBtn = document.querySelector(nextBtnId);

        if (prevBtn) {
            if (this.config.currentPage === 1 || this.state.totalPages <= 1) {
                prevBtn.classList.add('disabled');
                prevBtn.disabled = true;
            } else {
                prevBtn.classList.remove('disabled');
                prevBtn.disabled = false;
            }
        }

        if (nextBtn) {
            if (this.config.currentPage === this.state.totalPages || this.state.totalPages <= 1) {
                nextBtn.classList.add('disabled');
                nextBtn.disabled = true;
            } else {
                nextBtn.classList.remove('disabled');
                nextBtn.disabled = false;
            }
        }
    },

    // Обновить состояние кнопки страницы
    updatePageButtonState(pageNumber, state) {
        const buttons = document.querySelectorAll(`.page-number-btn[data-page="${pageNumber}"]`);
        buttons.forEach(button => {
            if (state === 'loading') {
                button.innerHTML = '<span class="spinner-border spinner-border-sm" role="status"></span>';
                button.disabled = true;
                button.classList.remove('btn-outline-primary', 'btn-outline-secondary', 'btn-outline-danger');
                button.classList.add('btn-secondary');
                button.title = 'Загрузка...';
            } else if (state === 'loaded') {
                button.textContent = pageNumber;
                button.disabled = false;
                button.classList.remove('btn-secondary', 'btn-outline-secondary', 'btn-outline-danger');
                button.classList.add('btn-outline-primary');
                button.title = 'Загружена (кликните для показа)';
            } else if (state === 'current') {
                button.textContent = pageNumber;
                button.classList.remove('btn-outline-primary', 'btn-outline-secondary', 'btn-secondary', 'btn-outline-danger');
                button.classList.add('btn-primary');
                button.title = 'Текущая страница';
            } else if (state === 'not-loaded') {
                button.textContent = pageNumber;
                button.disabled = false;
                button.classList.remove('btn-primary', 'btn-outline-primary', 'btn-secondary', 'btn-outline-danger');
                button.classList.add('btn-outline-secondary');
                button.title = 'Еще не загружена (кликните для загрузки)';
            } else if (state === 'error') {
                button.textContent = pageNumber;
                button.disabled = false;
                button.classList.remove('btn-primary', 'btn-outline-primary', 'btn-secondary');
                button.classList.add('btn-outline-danger');
                button.title = 'Ошибка загрузки (попробуйте снова)';
            }
        });
    },

    // Скрыть пагинацию
    hidePagination() {
        const topContainer = document.querySelector(this.config.paginationTopSelector);
        const bottomContainer = document.querySelector(this.config.paginationBottomSelector);

        if (topContainer) {
            topContainer.style.display = 'none';
        }
        if (bottomContainer) {
            bottomContainer.style.display = 'none';
        }
    },

    // Показать пагинацию
    showPagination() {
        const topContainer = document.querySelector(this.config.paginationTopSelector);
        const bottomContainer = document.querySelector(this.config.paginationBottomSelector);

        if (topContainer) {
            topContainer.style.display = 'block';
        }
        if (bottomContainer) {
            bottomContainer.style.display = 'block';
        }
    },

    // Обновить пагинацию после изменений
    updateAfterChanges() {
        console.log('Updating lazy pagination after changes...');

        this.loadPaginationInfoFromDOM();

        // Всегда показываем пагинацию
        this.showPagination();

        if (!document.querySelector(this.config.pageNumbersContainerTopId)) {
            this.createPageNumbersContainer('top');
        }
        if (!document.querySelector(this.config.pageNumbersContainerBottomId)) {
            this.createPageNumbersContainer('bottom');
        }

        this.setupPagination();
        this.showPage(1, false);
    },

    // Сбросить к первой странице
    resetToFirstPage() {
        this.showPage(1, false);
    },

    // Принудительно обновить
    forceUpdate() {
        console.log('Force updating lazy pagination...');

        this.loadPaginationInfoFromDOM();
        this.loadCurrentGamesFromDOM();

        console.log(`Force update: ${this.state.totalItems} items, ${this.state.totalPages} pages`);

        // Всегда показываем пагинацию
        this.showPagination();

        this.createPageNumbersContainer('top');
        this.createPageNumbersContainer('bottom');

        this.setupPagination();
        this.showPage(1, false);
    },

    // Получить текущую страницу
    getCurrentPage() {
        return this.config.currentPage;
    },

    // Получить общее количество страниц
    getTotalPages() {
        return this.state.totalPages;
    },

    // Проверить есть ли пагинация
    hasPagination() {
        return this.state.totalPages > 1;
    },

    // Запустить фоновую загрузку всех страниц
    startBackgroundLoading() {
        console.log('Starting background page loading...');

        // Загружаем следующую страницу сразу
        if (this.state.totalPages > 1) {
            this.loadPageInBackground(2);
        }

        // Остальные страницы загружаем с задержкой
        for (let page = 3; page <= this.state.totalPages; page++) {
            setTimeout(() => {
                this.loadPageInBackground(page);
            }, (page - 2) * 2000); // 2 секунды между загрузками
        }
    },

    // Загрузить страницу в фоне
    loadPageInBackground(pageNumber) {
        if (this.state.loadedPages.has(pageNumber)) {
            console.log(`Page ${pageNumber} already loaded, skipping...`);
            return;
        }

        console.log(`Loading page ${pageNumber} in background...`);

        // Сначала проверяем кэш
        const cachedGames = this.getPageFromStorage(pageNumber);
        if (cachedGames && cachedGames.length > 0) {
            console.log(`Found page ${pageNumber} in cache`);
            this.processPageGames(pageNumber, cachedGames);
            return;
        }

        // Загружаем с сервера
        this.loadPageFromServer(pageNumber)
            .then(games => {
                this.processPageGames(pageNumber, games);
                console.log(`Page ${pageNumber} loaded in background`);
            })
            .catch(error => {
                console.error(`Error loading page ${pageNumber} in background:`, error);
            });
    },

    // Деструктор для очистки
    destroy() {
        console.log('Destroying lazy game pagination...');
        this.state.gameItems = [];
        this.state.loadedPages.clear();
    }
};

document.addEventListener('DOMContentLoaded', () => {
    setTimeout(() => {
        console.log('DOM loaded, initializing lazy game pagination...');
        GamePagination.init();
    }, 500);
});

window.addEventListener('load', () => {
    setTimeout(() => {
        console.log('Page fully loaded, checking lazy game pagination...');
        if (!GamePagination.state.gameItems || GamePagination.state.gameItems.length === 0) {
            GamePagination.forceUpdate();
        }

        // Запускаем фоновую загрузку после полной загрузки страницы
        setTimeout(() => {
            if (GamePagination.hasPagination()) {
                GamePagination.startBackgroundLoading();
            }
        }, 2000);
    }, 1000);
});

export default GamePagination;