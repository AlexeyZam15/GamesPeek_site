// games/static/games/js/modules/game-pagination.js

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

    // Инициализация
    init() {
        console.log('Initializing games pagination...');

        this.gameItems = document.querySelectorAll(this.config.itemsSelector);
        this.totalItems = this.gameItems.length;
        this.totalPages = Math.ceil(this.totalItems / this.config.itemsPerPage);

        if (this.totalItems <= this.config.itemsPerPage) {
            console.log(`Only ${this.totalItems} games, no pagination needed.`);
            this.hidePagination();
            // Показываем все элементы
            this.gameItems.forEach(item => item.style.display = 'block');
            return;
        }

        // Создаем контейнеры для номеров страниц (верх и низ)
        this.createPageNumbersContainer('top');
        this.createPageNumbersContainer('bottom');

        this.setupPagination();
        this.showPage(1);

        console.log(`Games pagination initialized: ${this.totalItems} items, ${this.totalPages} pages`);
    },

    // Создаем контейнер для номеров страниц
    createPageNumbersContainer(position) {
        const containerId = position === 'top' ?
            this.config.pageNumbersContainerTopId :
            this.config.pageNumbersContainerBottomId;

        const buttonId = position === 'top' ?
            this.config.prevButtonTopId :
            this.config.prevButtonBottomId;

        // Уже существует?
        if (document.querySelector(containerId)) return;

        const paginationContainer = document.querySelector(
            position === 'top' ?
            this.config.paginationTopSelector :
            this.config.paginationBottomSelector
        );
        if (!paginationContainer) return;

        const prevButton = document.querySelector(buttonId);
        if (!prevButton) return;

        // Создаем контейнер для номеров страниц
        const pageNumbersContainer = document.createElement('div');
        pageNumbersContainer.id = containerId.substring(1); // Убираем #
        pageNumbersContainer.className = 'd-flex gap-1 align-items-center';
        pageNumbersContainer.style.minWidth = '200px';
        pageNumbersContainer.style.justifyContent = 'center';

        // Вставляем контейнер после кнопки "предыдущая"
        prevButton.parentNode.insertBefore(pageNumbersContainer, prevButton.nextSibling);
    },

    // Настройка пагинации
    setupPagination() {
        this.updatePageNumbers();
        this.setupNavigationButtons();
        this.updatePageInfo();
    },

    // Обновить номера страниц для обеих пагинаций
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

        // Очищаем контейнер
        pageNumbersContainer.innerHTML = '';

        // Определяем диапазон страниц для показа
        const { startPage, endPage } = this.getVisiblePageRange(this.config.currentPage);

        // Добавляем первую страницу если она не в диапазоне
        if (startPage > 1) {
            this.createPageNumberButton(pageNumbersContainer, 1, position);

            // Добавляем многоточие если нужно
            if (startPage > 2) {
                this.createEllipsis(pageNumbersContainer);
            }
        }

        // Добавляем страницы в диапазоне
        for (let i = startPage; i <= endPage; i++) {
            this.createPageNumberButton(pageNumbersContainer, i, position);
        }

        // Добавляем последнюю страницу если она не в диапазоне
        if (endPage < this.totalPages) {
            // Добавляем многоточие если нужно
            if (endPage < this.totalPages - 1) {
                this.createEllipsis(pageNumbersContainer);
            }

            this.createPageNumberButton(pageNumbersContainer, this.totalPages, position);
        }
    },

    // Создать кнопку номера страницы
    createPageNumberButton(container, pageNumber, position) {
        const button = document.createElement('button');
        button.type = 'button';
        button.className = 'btn btn-sm page-number-btn';

        if (pageNumber === this.config.currentPage) {
            button.classList.add('btn-primary');
        } else {
            button.classList.add('btn-outline-primary');
        }

        button.textContent = pageNumber;
        button.dataset.page = pageNumber;
        button.dataset.position = position;

        button.addEventListener('click', (e) => {
            e.preventDefault();
            this.showPage(pageNumber);
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
        // Для небольшого количества страниц показываем все
        if (this.totalPages <= this.config.maxVisiblePages) {
            return { startPage: 1, endPage: this.totalPages };
        }

        let startPage = currentPage - Math.floor(this.config.maxVisiblePages / 2);
        let endPage = currentPage + Math.floor(this.config.maxVisiblePages / 2);

        // Корректируем если вышли за границы
        if (startPage < 1) {
            startPage = 1;
            endPage = this.config.maxVisiblePages;
        }

        if (endPage > this.totalPages) {
            endPage = this.totalPages;
            startPage = endPage - this.config.maxVisiblePages + 1;
        }

        return { startPage, endPage };
    },

    // Настройка кнопок навигации
    setupNavigationButtons() {
        // Верхние кнопки
        this.setupNavigationButtonPair('top');
        // Нижние кнопки
        this.setupNavigationButtonPair('bottom');
    },

    // Настройка пары кнопок (prev/next)
    setupNavigationButtonPair(position) {
        const prevBtnId = position === 'top' ? this.config.prevButtonTopId : this.config.prevButtonBottomId;
        const nextBtnId = position === 'top' ? this.config.nextButtonTopId : this.config.nextButtonBottomId;

        const prevBtn = document.querySelector(prevBtnId);
        const nextBtn = document.querySelector(nextBtnId);

        if (prevBtn) {
            // Удаляем старые обработчики
            const newPrevBtn = prevBtn.cloneNode(true);
            prevBtn.parentNode.replaceChild(newPrevBtn, prevBtn);

            newPrevBtn.addEventListener('click', (e) => {
                e.preventDefault();
                if (this.config.currentPage > 1) {
                    this.showPage(this.config.currentPage - 1);
                }
            });
        }

        if (nextBtn) {
            // Удаляем старые обработчики
            const newNextBtn = nextBtn.cloneNode(true);
            nextBtn.parentNode.replaceChild(newNextBtn, nextBtn);

            newNextBtn.addEventListener('click', (e) => {
                e.preventDefault();
                if (this.config.currentPage < this.totalPages) {
                    this.showPage(this.config.currentPage + 1);
                }
            });
        }
    },

    // Показать определенную страницу
    showPage(pageNumber) {
        if (pageNumber < 1 || pageNumber > this.totalPages) return;

        this.config.currentPage = pageNumber;

        // Показываем/скрываем элементы
        const startIndex = (pageNumber - 1) * this.config.itemsPerPage;
        const endIndex = Math.min(startIndex + this.config.itemsPerPage, this.totalItems);

        this.gameItems.forEach((item, index) => {
            if (index >= startIndex && index < endIndex) {
                item.style.display = 'block';
            } else {
                item.style.display = 'none';
            }
        });

        // Обновляем номера страниц
        this.updatePageNumbers();

        // Обновляем информацию
        this.updatePageInfo();

        // Обновляем кнопки навигации
        this.updateNavigationButtons();

        console.log(`Showing games page ${pageNumber} (items ${startIndex + 1}-${endIndex})`);
    },

    // Обновить информацию о странице
    updatePageInfo() {
        const startIndex = (this.config.currentPage - 1) * this.config.itemsPerPage + 1;
        const endIndex = Math.min(this.config.currentPage * this.config.itemsPerPage, this.totalItems);

        // Обновляем верхнюю пагинацию
        this.updateSinglePageInfo('top', startIndex, endIndex);
        // Обновляем нижнюю пагинацию
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
        if (totalElement) totalElement.textContent = this.totalItems;
        if (totalPagesElement) totalPagesElement.textContent = this.totalPages;
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
            if (this.config.currentPage === 1) {
                prevBtn.classList.add('disabled');
            } else {
                prevBtn.classList.remove('disabled');
            }
        }

        if (nextBtn) {
            if (this.config.currentPage === this.totalPages) {
                nextBtn.classList.add('disabled');
            } else {
                nextBtn.classList.remove('disabled');
            }
        }
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

    // Обновить пагинацию после изменений
    updateAfterChanges() {
        // Получаем текущие видимые элементы
        this.gameItems = document.querySelectorAll(this.config.itemsSelector);
        this.totalItems = this.gameItems.length;
        this.totalPages = Math.ceil(this.totalItems / this.config.itemsPerPage);

        if (this.totalItems <= this.config.itemsPerPage) {
            this.hidePagination();
            // Показываем все элементы
            this.gameItems.forEach(item => item.style.display = 'block');
        } else {
            // Показываем пагинацию
            const topContainer = document.querySelector(this.config.paginationTopSelector);
            const bottomContainer = document.querySelector(this.config.paginationBottomSelector);

            if (topContainer) {
                topContainer.style.display = 'block';
            }
            if (bottomContainer) {
                bottomContainer.style.display = 'block';
            }

            // Перестраиваем пагинацию
            this.setupPagination();
            this.showPage(1);
        }
    },

    // Сбросить к первой странице
    resetToFirstPage() {
        this.showPage(1);
    },

    // Принудительно обновить после изменений DOM
    forceUpdate() {
        // Пересчитываем элементы
        this.gameItems = document.querySelectorAll(this.config.itemsSelector);
        this.totalItems = this.gameItems.length;
        this.totalPages = Math.ceil(this.totalItems / this.config.itemsPerPage);

        if (this.totalItems <= this.config.itemsPerPage) {
            this.hidePagination();
            // Показываем все элементы
            this.gameItems.forEach(item => item.style.display = 'block');
        } else {
            // Создаем контейнеры если их нет
            this.createPageNumbersContainer('top');
            this.createPageNumbersContainer('bottom');

            this.setupPagination();
            this.showPage(1);
        }
    }
};

// Экспорт для использования в других модулях
export default GamePagination;