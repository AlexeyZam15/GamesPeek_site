// games/static/games/js/game_list/keywords-pagination.js

// Добавляем служебный объект для таймеров
const KeywordsPaginationDebugTimer = {
    marks: {},
    start(label) {
        this.marks[label] = performance.now();
    },
    end(label) {
        const endTime = performance.now();
        const startTime = this.marks[label];
        if (startTime) {
            const duration = (endTime - startTime).toFixed(2);
            console.warn(`[TIMER] KeywordsPagination: ${label} took ${duration} ms`);
            delete this.marks[label];
        } else {
            console.warn(`[TIMER] KeywordsPagination: No start mark found for: ${label}`);
        }
    }
};

const KeywordsPagination = {
    // Конфигурация
    config: {
        itemsPerPage: 30,
        itemsSelector: '.keyword-item, .search-keyword-item'
    },

    // Хранилище для состояний разных контейнеров
    containers: {},

    // Инициализация - создает пагинацию для ВСЕХ контейнеров
    init() {
        KeywordsPaginationDebugTimer.start('init');
        console.log('🚀 KeywordsPagination: Initializing for all containers...');

        // Находим similarity контейнер
        const similarityContainer = document.querySelector('.keyword-list');
        if (similarityContainer) {
            console.log('KeywordsPagination: Found similarity container with items:', similarityContainer.querySelectorAll('.keyword-item').length);
            this.initForContainer(similarityContainer, 'similarity');
        } else {
            console.log('KeywordsPagination: Similarity container (.keyword-list) not found');
        }

        // Находим search контейнер
        const searchContainer = document.querySelector('.search-keyword-list');
        if (searchContainer) {
            console.log('KeywordsPagination: Found search container with items:', searchContainer.querySelectorAll('.search-keyword-item').length);
            this.initForContainer(searchContainer, 'search');
        } else {
            console.log('KeywordsPagination: Search container (.search-keyword-list) not found');
        }

        KeywordsPaginationDebugTimer.end('init');
    },

    // Инициализация для конкретного контейнера
    initForContainer(keywordContainer, containerType) {
        console.log(`KeywordsPagination: Initializing ${containerType} container`);

        // Получаем все элементы ключевых слов
        const keywordItems = keywordContainer.querySelectorAll('.keyword-item, .search-keyword-item');
        const totalItems = keywordItems.length;

        console.log(`KeywordsPagination: ${containerType} - found ${totalItems} keyword items`);

        // Если элементов мало, не создаем пагинацию
        if (totalItems <= this.config.itemsPerPage) {
            console.log(`KeywordsPagination: ${containerType} - only ${totalItems} keywords, pagination not needed`);
            // Показываем все элементы
            keywordItems.forEach(item => {
                item.style.display = 'block';
            });
            return;
        }

        console.log(`KeywordsPagination: ${containerType} - ${totalItems} keywords, setting up pagination...`);

        // Удаляем старую пагинацию для этого контейнера
        const existingPagination = keywordContainer.parentNode.querySelector('.keyword-pagination');
        if (existingPagination) {
            existingPagination.remove();
            console.log(`KeywordsPagination: ${containerType} - removed existing pagination`);
        }

        // Создаем пагинацию для этого контейнера
        this.createPaginationForContainer(keywordContainer, containerType, keywordItems, totalItems);
    },

    // Создание элементов пагинации для конкретного контейнера
    createPaginationForContainer(keywordContainer, containerType, keywordItems, totalItems) {
        KeywordsPaginationDebugTimer.start('createPaginationForContainer');
        console.log(`KeywordsPagination: Creating pagination for ${containerType}`);

        const totalPages = Math.ceil(totalItems / this.config.itemsPerPage);
        const originalItems = Array.from(keywordItems);

        // Создаем контейнер для пагинации
        const paginationContainer = document.createElement('div');
        paginationContainer.className = `keyword-pagination keyword-pagination-${containerType} mt-3`;
        paginationContainer.innerHTML = `
            <nav aria-label="Keyword pagination" class="d-flex flex-column align-items-center">
                <div class="d-flex align-items-center justify-content-center gap-2 mb-2">
                    <button type="button" class="btn btn-sm btn-outline-primary keyword-prev-btn-${containerType}">
                        <i class="bi bi-chevron-left"></i>
                    </button>
                    <div class="keyword-page-numbers-${containerType} d-flex flex-wrap gap-1 justify-content-center"></div>
                    <button type="button" class="btn btn-sm btn-outline-primary keyword-next-btn-${containerType}">
                        <i class="bi bi-chevron-right"></i>
                    </button>
                </div>
                <div class="text-center">
                    <small class="text-muted keyword-page-info-${containerType}">
                        Page <span class="keyword-current-page-${containerType}">1</span>
                        of <span class="keyword-total-pages-${containerType}">${totalPages}</span>
                        (<span class="keyword-start-index-${containerType}">1</span>-<span class="keyword-end-index-${containerType}">${Math.min(this.config.itemsPerPage, totalItems)}</span>
                        of <span class="keyword-total-items-${containerType}">${totalItems}</span> keywords)
                    </small>
                </div>
            </nav>
        `;

        // Вставляем пагинацию после контейнера с ключевыми словами
        keywordContainer.parentNode.insertBefore(paginationContainer, keywordContainer.nextSibling);

        console.log(`KeywordsPagination: Pagination HTML inserted for ${containerType}`);

        // Сохраняем состояние для этого контейнера
        const containerState = {
            container: keywordContainer,
            containerType: containerType,
            totalItems: totalItems,
            totalPages: totalPages,
            currentPage: 1,
            originalItems: originalItems,
            paginationElement: paginationContainer,
            showAllButton: this.findShowAllButtonForContainer(containerType)
        };

        // Показываем первую страницу
        this.showPageForContainer(containerState, 1);

        // Сохраняем состояние
        this.containers[containerType] = containerState;

        console.log(`KeywordsPagination: Pagination created for ${containerType}`);
        KeywordsPaginationDebugTimer.end('createPaginationForContainer');
    },

    // Поиск кнопки "Show all features" для контейнера
    findShowAllButtonForContainer(containerType) {
        if (containerType === 'search') {
            return document.querySelector('.search-show-all-keywords-btn');
        } else {
            return document.querySelector('.show-all-keywords-btn');
        }
    },

    // Показать страницу для конкретного контейнера
    showPageForContainer(state, pageNumber) {
        if (pageNumber < 1 || pageNumber > state.totalPages) {
            return;
        }

        console.log(`KeywordsPagination: ${state.containerType} - showing page ${pageNumber}`);

        state.currentPage = pageNumber;
        const startIndex = (pageNumber - 1) * this.config.itemsPerPage;
        const endIndex = Math.min(startIndex + this.config.itemsPerPage, state.totalItems);

        // Скрываем все элементы
        state.originalItems.forEach(item => {
            item.style.display = 'none';
        });

        // Показываем элементы текущей страницы
        for (let i = startIndex; i < endIndex; i++) {
            if (state.originalItems[i]) {
                state.originalItems[i].style.display = 'block';
            }
        }

        // Обновляем UI
        this.updatePageInfoForContainer(state, startIndex, endIndex);
        this.updatePageNumbersForContainer(state);
        this.updateNavigationButtonsForContainer(state);

        // Прокручиваем к началу списка
        if (state.container) {
            state.container.scrollTop = 0;
        }

        console.log(`KeywordsPagination: ${state.containerType} - showing items ${startIndex + 1}-${endIndex} of ${state.totalItems}`);
    },

    // Обновить информацию о странице для контейнера
    updatePageInfoForContainer(state, startIndex, endIndex) {
        const currentPageEl = document.querySelector(`.keyword-current-page-${state.containerType}`);
        const totalPagesEl = document.querySelector(`.keyword-total-pages-${state.containerType}`);
        const startIndexEl = document.querySelector(`.keyword-start-index-${state.containerType}`);
        const endIndexEl = document.querySelector(`.keyword-end-index-${state.containerType}`);
        const totalItemsEl = document.querySelector(`.keyword-total-items-${state.containerType}`);

        if (currentPageEl) currentPageEl.textContent = state.currentPage;
        if (totalPagesEl) totalPagesEl.textContent = state.totalPages;
        if (startIndexEl) startIndexEl.textContent = startIndex + 1;
        if (endIndexEl) endIndexEl.textContent = endIndex;
        if (totalItemsEl) totalItemsEl.textContent = state.totalItems;
    },

    // Обновить номера страниц для контейнера
    updatePageNumbersForContainer(state) {
        const pageNumbersContainer = document.querySelector(`.keyword-page-numbers-${state.containerType}`);
        if (!pageNumbersContainer) return;

        pageNumbersContainer.innerHTML = '';

        if (state.totalPages <= 1) {
            return;
        }

        let startPage = Math.max(1, state.currentPage - 2);
        let endPage = Math.min(state.totalPages, state.currentPage + 2);

        if (endPage - startPage < 4) {
            if (startPage === 1) {
                endPage = Math.min(state.totalPages, startPage + 4);
            } else if (endPage === state.totalPages) {
                startPage = Math.max(1, endPage - 4);
            }
        }

        if (startPage > 1) {
            const firstBtn = this.createPageButtonForContainer(state, 1);
            pageNumbersContainer.appendChild(firstBtn);

            if (startPage > 2) {
                const ellipsis = document.createElement('span');
                ellipsis.className = 'text-muted mx-1';
                ellipsis.textContent = '...';
                ellipsis.style.fontWeight = 'bold';
                pageNumbersContainer.appendChild(ellipsis);
            }
        }

        for (let i = startPage; i <= endPage; i++) {
            const pageBtn = this.createPageButtonForContainer(state, i);
            pageNumbersContainer.appendChild(pageBtn);
        }

        if (endPage < state.totalPages) {
            if (endPage < state.totalPages - 1) {
                const ellipsis = document.createElement('span');
                ellipsis.className = 'text-muted mx-1';
                ellipsis.textContent = '...';
                ellipsis.style.fontWeight = 'bold';
                pageNumbersContainer.appendChild(ellipsis);
            }

            const lastBtn = this.createPageButtonForContainer(state, state.totalPages);
            pageNumbersContainer.appendChild(lastBtn);
        }
    },

    // Создать кнопку страницы для контейнера
    createPageButtonForContainer(state, pageNumber) {
        const button = document.createElement('button');
        button.type = 'button';
        button.className = `btn btn-sm page-number-btn ${pageNumber === state.currentPage ? 'btn-primary' : 'btn-outline-secondary'}`;
        button.textContent = pageNumber;
        button.title = `Page ${pageNumber}`;

        button.style.border = '1px solid var(--border, rgba(255, 107, 53, 0.2))';
        button.style.borderRadius = '6px';
        button.style.padding = '0.5rem 1rem';
        button.style.fontWeight = '600';
        button.style.minWidth = '2.5rem';
        button.style.height = '2.5rem';
        button.style.transition = 'all 0.2s ease';

        if (pageNumber === state.currentPage) {
            button.style.background = 'linear-gradient(135deg, var(--secondary-color, #ff6b35), var(--accent-color, #ff8e53))';
            button.style.borderColor = 'var(--secondary-color, #ff6b35)';
            button.style.color = 'white';
            button.style.boxShadow = '0 4px 12px rgba(255, 107, 53, 0.3)';
        } else {
            button.style.backgroundColor = 'var(--surface, rgba(26, 26, 26, 0.4))';
            button.style.color = 'var(--text-dark, white)';
        }

        button.addEventListener('click', () => {
            this.showPageForContainer(state, pageNumber);
        });

        return button;
    },

    // Обновить кнопки навигации для контейнера
    updateNavigationButtonsForContainer(state) {
        const prevBtn = document.querySelector(`.keyword-prev-btn-${state.containerType}`);
        const nextBtn = document.querySelector(`.keyword-next-btn-${state.containerType}`);

        if (prevBtn) {
            const isDisabled = state.currentPage === 1;
            prevBtn.disabled = isDisabled;
            prevBtn.classList.toggle('disabled', isDisabled);

            const newPrevBtn = prevBtn.cloneNode(true);
            prevBtn.parentNode.replaceChild(newPrevBtn, prevBtn);
            newPrevBtn.addEventListener('click', () => {
                if (state.currentPage > 1) {
                    this.showPageForContainer(state, state.currentPage - 1);
                }
            });
        }

        if (nextBtn) {
            const isDisabled = state.currentPage === state.totalPages;
            nextBtn.disabled = isDisabled;
            nextBtn.classList.toggle('disabled', isDisabled);

            const newNextBtn = nextBtn.cloneNode(true);
            nextBtn.parentNode.replaceChild(newNextBtn, nextBtn);
            newNextBtn.addEventListener('click', () => {
                if (state.currentPage < state.totalPages) {
                    this.showPageForContainer(state, state.currentPage + 1);
                }
            });
        }
    },

    // Переключение режима "Show all" для контейнера
    toggleShowAllForContainer(state) {
        if (!state.isShowingAll) {
            state.isShowingAll = true;

            // Показываем все элементы
            state.originalItems.forEach(item => {
                item.style.display = 'block';
            });

            // Скрываем пагинацию
            if (state.paginationElement) {
                state.paginationElement.style.display = 'none';
            }

            // Меняем текст кнопки
            if (state.showAllButton) {
                const showText = state.showAllButton.querySelector('.show-text');
                const hideText = state.showAllButton.querySelector('.hide-text');
                if (showText) showText.style.display = 'none';
                if (hideText) hideText.style.display = 'inline';
                state.showAllButton.style.backgroundColor = 'var(--secondary-color, #ff6b35)';
                state.showAllButton.style.color = 'white';
            }

            // Убираем ограничение высоты
            if (state.container) {
                state.container.style.maxHeight = 'none';
                state.container.style.overflowY = 'visible';
            }
        } else {
            state.isShowingAll = false;

            // Показываем пагинацию
            if (state.paginationElement) {
                state.paginationElement.style.display = 'block';
            }

            // Восстанавливаем кнопку
            if (state.showAllButton) {
                const showText = state.showAllButton.querySelector('.show-text');
                const hideText = state.showAllButton.querySelector('.hide-text');
                if (showText) showText.style.display = 'inline';
                if (hideText) hideText.style.display = 'none';
                state.showAllButton.style.backgroundColor = '';
                state.showAllButton.style.color = '';
            }

            // Показываем первую страницу
            this.showPageForContainer(state, 1);

            // Восстанавливаем ограничение высоты
            if (state.container) {
                state.container.style.maxHeight = '400px';
                state.container.style.overflowY = 'auto';
            }
        }
    },

    // Обновить после поиска (для всех контейнеров)
    updateAfterSearch() {
        console.log('KeywordsPagination: Updating after search...');
        for (const [type, state] of Object.entries(this.containers)) {
            this.updateContainerAfterSearch(state);
        }
    },

    // Обновить конкретный контейнер после поиска
    updateContainerAfterSearch(state) {
        if (!state.container) return;

        const visibleItems = state.container.querySelectorAll('.keyword-item[style*="block"], .search-keyword-item[style*="block"]');
        const totalVisible = visibleItems.length;

        console.log(`KeywordsPagination: ${state.containerType} - ${totalVisible} visible items after search`);

        if (totalVisible <= this.config.itemsPerPage) {
            if (state.paginationElement) {
                state.paginationElement.style.display = 'none';
            }
        } else {
            if (state.paginationElement) {
                state.paginationElement.style.display = 'block';
            }

            state.totalItems = totalVisible;
            state.totalPages = Math.ceil(totalVisible / this.config.itemsPerPage);
            state.currentPage = 1;
            state.originalItems = Array.from(visibleItems);

            this.showPageForContainer(state, 1);
        }
    },

    // Принудительное обновление
    forceUpdate() {
        console.log('KeywordsPagination: Force updating...');
        this.containers = {};
        this.init();
    },

    // Полное обновление
    refresh() {
        console.log('KeywordsPagination: Refreshing...');
        this.containers = {};
        this.init();
    },

    // Деструктор
    destroy() {
        console.log('KeywordsPagination: Destroying...');
        this.containers = {};
    }
};

// Экспорт для глобального использования
if (typeof module !== 'undefined' && module.exports) {
    module.exports = KeywordsPagination;
} else {
    window.KeywordsPagination = KeywordsPagination;
}

// Автоматическая инициализация
document.addEventListener('DOMContentLoaded', function() {
    console.log('KeywordsPagination: DOM loaded, initializing...');
    setTimeout(() => {
        KeywordsPagination.init();
    }, 500);
});

// Слушаем событие обновления AJAX контента
document.addEventListener('ajax-content-loaded', function() {
    console.log('KeywordsPagination: AJAX content loaded, refreshing...');
    setTimeout(() => {
        KeywordsPagination.refresh();
    }, 200);
});

export default KeywordsPagination;