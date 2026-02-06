// games/static/games/js/game_list/game-pagination-ui.js
// UI класс для отображения пагинации

const GamePaginationUI = {
    // Конфигурация UI
    config: {
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

    // Инициализация UI
    init() {
        console.log('Initializing GamePagination UI...');

        this.createPageNumbersContainer('top');
        this.createPageNumbersContainer('bottom');

        this.setupPagination();
        this.updatePageInfo();
        this.updateNavigationButtons();

        console.log('GamePagination UI initialized successfully');
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

        // Если нет данных о пагинации, выходим
        if (!window.GamePagination || window.GamePagination.state.totalPages === 0) {
            return;
        }

        // Если всего 1 страница, показываем только её
        if (window.GamePagination.state.totalPages <= 1) {
            this.createPageNumberButton(pageNumbersContainer, 1, position);
            return;
        }

        const { startPage, endPage } = this.getVisiblePageRange(window.GamePagination.config.currentPage);

        if (startPage > 1) {
            this.createPageNumberButton(pageNumbersContainer, 1, position);

            if (startPage > 2) {
                this.createEllipsis(pageNumbersContainer);
            }
        }

        for (let i = startPage; i <= endPage; i++) {
            this.createPageNumberButton(pageNumbersContainer, i, position);
        }

        if (endPage < window.GamePagination.state.totalPages) {
            if (endPage < window.GamePagination.state.totalPages - 1) {
                this.createEllipsis(pageNumbersContainer);
            }

            this.createPageNumberButton(pageNumbersContainer, window.GamePagination.state.totalPages, position);
        }
    },

    // Создать кнопку номера страницы
    createPageNumberButton(container, pageNumber, position) {
        const button = document.createElement('button');
        button.type = 'button';
        button.className = 'btn btn-sm page-number-btn';

        if (pageNumber === window.GamePagination.config.currentPage) {
            button.classList.add('btn-primary');
            button.title = 'Текущая страница';
        } else if (window.GamePagination.state.loadedPages.has(pageNumber)) {
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
            e.stopPropagation();

            if (!window.GamePagination.state.loadedPages.has(pageNumber)) {
                window.GamePagination.forceLoadPage(pageNumber);
            } else {
                window.GamePagination.showPage(pageNumber, false);
            }
        });

        container.appendChild(button);
        return button;
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
        const totalPages = window.GamePagination.state.totalPages;

        // Для небольшого количества страниц показываем все
        if (totalPages <= this.config.maxVisiblePages) {
            return { startPage: 1, endPage: totalPages };
        }

        let startPage = currentPage - Math.floor(this.config.maxVisiblePages / 2);
        let endPage = currentPage + Math.floor(this.config.maxVisiblePages / 2);

        // Корректируем если вышли за границы
        if (startPage < 1) {
            startPage = 1;
            endPage = this.config.maxVisiblePages;
        }

        if (endPage > totalPages) {
            endPage = totalPages;
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
                e.stopPropagation();
                if (window.GamePagination.config.currentPage > 1) {
                    const prevPage = window.GamePagination.config.currentPage - 1;
                    if (!window.GamePagination.state.loadedPages.has(prevPage)) {
                        window.GamePagination.forceLoadPage(prevPage);
                    } else {
                        window.GamePagination.showPage(prevPage, false);
                    }
                }
            });
        }

        if (nextBtn) {
            const newNextBtn = nextBtn.cloneNode(true);
            nextBtn.parentNode.replaceChild(newNextBtn, nextBtn);

            newNextBtn.addEventListener('click', (e) => {
                e.preventDefault();
                e.stopPropagation();
                if (window.GamePagination.config.currentPage < window.GamePagination.state.totalPages) {
                    const nextPage = window.GamePagination.config.currentPage + 1;
                    if (!window.GamePagination.state.loadedPages.has(nextPage)) {
                        window.GamePagination.forceLoadPage(nextPage);
                    } else {
                        window.GamePagination.showPage(nextPage, false);
                    }
                }
            });
        }
    },

    // Обновить информацию о странице
    updatePageInfo() {
        if (!window.GamePagination) return;

        const startIndex = (window.GamePagination.config.currentPage - 1) * window.GamePagination.config.itemsPerPage + 1;
        const endIndex = Math.min(window.GamePagination.config.currentPage * window.GamePagination.config.itemsPerPage, window.GamePagination.state.totalItems);

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
        if (currentElement) currentElement.textContent = window.GamePagination.config.currentPage;
        if (totalElement) totalElement.textContent = window.GamePagination.state.totalItems;
        if (totalPagesElement) totalPagesElement.textContent = window.GamePagination.state.totalPages;
    },

    // Обновить элементы информации о странице
    updatePageInfoElements(currentPage, totalItems, totalPages, itemsPerPage) {
        const startIndex = (currentPage - 1) * itemsPerPage + 1;
        const endIndex = Math.min(currentPage * itemsPerPage, totalItems);

        // Обновляем верхние элементы
        const topElements = {
            start: document.querySelector(this.config.startElementTopId),
            end: document.querySelector(this.config.endElementTopId),
            current: document.querySelector(this.config.currentElementTopId),
            total: document.querySelector(this.config.totalElementTopId),
            totalPages: document.querySelector(this.config.totalPagesElementTopId)
        };

        // Обновляем нижние элементы
        const bottomElements = {
            start: document.querySelector(this.config.startElementBottomId),
            end: document.querySelector(this.config.endElementBottomId),
            current: document.querySelector(this.config.currentElementBottomId),
            total: document.querySelector(this.config.totalElementBottomId),
            totalPages: document.querySelector(this.config.totalPagesElementBottomId)
        };

        const updateElementSet = (elements) => {
            if (elements.start) elements.start.textContent = startIndex;
            if (elements.end) elements.end.textContent = endIndex;
            if (elements.current) elements.current.textContent = currentPage;
            if (elements.total) elements.total.textContent = totalItems;
            if (elements.totalPages) elements.totalPages.textContent = totalPages;
        };

        updateElementSet(topElements);
        updateElementSet(bottomElements);
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
            if (window.GamePagination.config.currentPage === 1 || window.GamePagination.state.totalPages <= 1) {
                prevBtn.classList.add('disabled');
                prevBtn.disabled = true;
            } else {
                prevBtn.classList.remove('disabled');
                prevBtn.disabled = false;
            }
        }

        if (nextBtn) {
            if (window.GamePagination.config.currentPage === window.GamePagination.state.totalPages || window.GamePagination.state.totalPages <= 1) {
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
    }
};

// Экспортируем для использования в других файлах
if (typeof module !== 'undefined' && module.exports) {
    module.exports = GamePaginationUI;
} else {
    window.GamePaginationUI = GamePaginationUI;
}

export default GamePaginationUI;