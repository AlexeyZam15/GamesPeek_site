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
            console.warn(`[TIMER] ${label} took ${duration} ms`);
            delete this.marks[label];
        } else {
            console.warn(`[TIMER] No start mark found for: ${label}`);
        }
    }
};

const KeywordsPagination = {
    // Конфигурация
    config: {
        itemsPerPage: 30,
        currentPage: 1,
        itemsSelector: '.keyword-item',
        containerSelector: '.keyword-list',
        containerId: 'keyword-list'
    },

    // Состояние
    state: {
        totalItems: 0,
        totalPages: 0,
        isInitialized: false,
        isShowingAll: false,
        originalItems: [],
        visibleItems: [],
        paginationElement: null,
        showAllButton: null
    },

    // Инициализация
    init() {
        KeywordsPaginationDebugTimer.start('KeywordsPagination.init');
        console.log('🚀 KeywordsPagination: Initializing...');

        // Находим контейнер с ключевыми словами
        const keywordContainer = document.querySelector('.keyword-list');
        if (!keywordContainer) {
            console.log('KeywordsPagination: Keyword container not found');
            KeywordsPaginationDebugTimer.end('KeywordsPagination.init');
            return;
        }

        // Присваиваем ID для идентификации
        keywordContainer.id = this.config.containerId;

        // Получаем все элементы ключевых слов
        const keywordItems = keywordContainer.querySelectorAll('.keyword-item');
        this.state.totalItems = keywordItems.length;

        if (this.state.totalItems <= this.config.itemsPerPage) {
            console.log(`KeywordsPagination: Only ${this.state.totalItems} keywords, pagination not needed`);
            KeywordsPaginationDebugTimer.end('KeywordsPagination.init');
            return;
        }

        console.log(`KeywordsPagination: Found ${this.state.totalItems} keywords, setting up pagination...`);

        // Сохраняем оригинальные элементы
        this.state.originalItems = Array.from(keywordItems);

        // Рассчитываем количество страниц
        this.state.totalPages = Math.ceil(this.state.totalItems / this.config.itemsPerPage);
        this.config.currentPage = 1;

        // Создаем пагинацию если ее нет
        this.createPaginationElements();

        // Находим кнопку "Show all features"
        this.findShowAllButton();

        // Показываем первую страницу
        this.showPage(this.config.currentPage);

        // Обновляем информацию о странице
        this.updatePageInfo();

        // Устанавливаем обработчики событий
        this.setupEventListeners();

        this.state.isInitialized = true;
        console.log(`KeywordsPagination: Initialized successfully with ${this.state.totalPages} pages`);
        KeywordsPaginationDebugTimer.end('KeywordsPagination.init');
    },

    // Создание элементов пагинации
    createPaginationElements() {
        console.log('KeywordsPagination: Creating pagination elements...');

        // Удаляем старую пагинацию если она есть
        this.removeExistingPagination();

        const keywordContainer = document.getElementById(this.config.containerId);
        if (!keywordContainer) return;

        // Создаем контейнер для пагинации
        const paginationContainer = document.createElement('div');
        paginationContainer.className = 'keyword-pagination mt-3';
        paginationContainer.innerHTML = `
            <nav aria-label="Keyword pagination" class="d-flex flex-column align-items-center">
                <div class="d-flex align-items-center justify-content-center gap-2 mb-2">
                    <button type="button" class="btn btn-sm btn-outline-primary keyword-prev-btn">
                        <i class="bi bi-chevron-left"></i>
                    </button>
                    <div class="keyword-page-numbers d-flex flex-wrap gap-1 justify-content-center"></div>
                    <button type="button" class="btn btn-sm btn-outline-primary keyword-next-btn">
                        <i class="bi bi-chevron-right"></i>
                    </button>
                </div>
                <div class="text-center">
                    <small class="text-muted keyword-page-info">
                        Page <span class="keyword-current-page">1</span>
                        of <span class="keyword-total-pages">${this.state.totalPages}</span>
                        (<span class="keyword-start-index">1</span>-<span class="keyword-end-index">${Math.min(this.config.itemsPerPage, this.state.totalItems)}</span>
                        of <span class="keyword-total-items">${this.state.totalItems}</span> keywords)
                    </small>
                </div>
            </nav>
        `;

        // Вставляем пагинацию после контейнера с ключевыми словами
        keywordContainer.parentNode.insertBefore(paginationContainer, keywordContainer.nextSibling);

        this.state.paginationElement = paginationContainer;
        console.log('KeywordsPagination: Pagination elements created');
    },

    // Удаление существующей пагинации
    removeExistingPagination() {
        const existingPagination = document.querySelector('.keyword-pagination');
        if (existingPagination) {
            existingPagination.remove();
            console.log('KeywordsPagination: Removed existing pagination');
        }
    },

    // Поиск кнопки "Show all features"
    findShowAllButton() {
        this.state.showAllButton = document.querySelector('.show-all-keywords-btn');
        if (this.state.showAllButton) {
            console.log('KeywordsPagination: Found "Show all features" button');

            // Устанавливаем обработчик
            const newButton = this.state.showAllButton.cloneNode(true);
            this.state.showAllButton.parentNode.replaceChild(newButton, this.state.showAllButton);
            this.state.showAllButton = newButton;

            newButton.addEventListener('click', (e) => {
                e.preventDefault();
                this.toggleShowAll();
            });

            // Добавляем оранжевую рамку для кнопки "Show all features"
            this.state.showAllButton.style.border = '2px solid var(--secondary-color, #ff6b35)';
            this.state.showAllButton.style.borderRadius = '20px';
            this.state.showAllButton.style.fontWeight = '700';
            this.state.showAllButton.style.padding = '0.4rem 1rem';
            this.state.showAllButton.style.transition = 'all 0.2s ease';

            // Hover эффект
            this.state.showAllButton.addEventListener('mouseenter', function() {
                this.style.backgroundColor = 'var(--secondary-color, #ff6b35)';
                this.style.color = 'white';
                this.style.transform = 'translateY(-2px)';
                this.style.boxShadow = '0 4px 8px rgba(255, 107, 53, 0.3)';
            });

            this.state.showAllButton.addEventListener('mouseleave', function() {
                this.style.backgroundColor = '';
                this.style.color = '';
                this.style.transform = '';
                this.style.boxShadow = '';
            });
        }
    },

    // Переключение режима "Show all"
    toggleShowAll() {
        this.state.isShowingAll = !this.state.isShowingAll;

        const keywordContainer = document.getElementById(this.config.containerId);
        const paginationElement = this.state.paginationElement;
        const showText = this.state.showAllButton?.querySelector('.show-text');
        const hideText = this.state.showAllButton?.querySelector('.hide-text');

        if (this.state.isShowingAll) {
            // Показываем все элементы
            this.state.originalItems.forEach(item => {
                item.style.display = 'block';
            });

            // Скрываем пагинацию
            if (paginationElement) {
                paginationElement.style.display = 'none';
            }

            // Обновляем текст кнопки
            if (showText) showText.style.display = 'none';
            if (hideText) hideText.style.display = 'inline';

            // Убираем ограничение высоты
            if (keywordContainer) {
                keywordContainer.style.maxHeight = 'none';
                keywordContainer.style.overflowY = 'visible';
            }

            // Меняем стиль кнопки
            if (this.state.showAllButton) {
                this.state.showAllButton.style.backgroundColor = 'var(--secondary-color, #ff6b35)';
                this.state.showAllButton.style.color = 'white';
            }

            console.log('KeywordsPagination: Showing all keywords');
        } else {
            // Возвращаем пагинацию
            if (paginationElement) {
                paginationElement.style.display = 'block';
            }

            // Восстанавливаем стиль кнопки
            if (this.state.showAllButton) {
                this.state.showAllButton.style.backgroundColor = '';
                this.state.showAllButton.style.color = '';
            }

            // Показываем первую страницу
            this.showPage(1);
            this.updatePageNumbers();

            // Обновляем текст кнопки
            if (showText) showText.style.display = 'inline';
            if (hideText) hideText.style.display = 'none';

            // Восстанавливаем ограничение высоты
            if (keywordContainer) {
                keywordContainer.style.maxHeight = '400px';
                keywordContainer.style.overflowY = 'auto';
            }

            console.log('KeywordsPagination: Showing paginated view');
        }
    },

    // Показать конкретную страницу
    showPage(pageNumber) {
        if (pageNumber < 1 || pageNumber > this.state.totalPages) {
            console.log(`KeywordsPagination: Page ${pageNumber} out of range (1-${this.state.totalPages})`);
            return;
        }

        console.log(`KeywordsPagination: Showing page ${pageNumber}`);

        this.config.currentPage = pageNumber;
        const startIndex = (pageNumber - 1) * this.config.itemsPerPage;
        const endIndex = Math.min(startIndex + this.config.itemsPerPage, this.state.totalItems);

        // Скрываем все элементы
        this.state.originalItems.forEach(item => {
            item.style.display = 'none';
        });

        // Показываем элементы текущей страницы
        for (let i = startIndex; i < endIndex; i++) {
            if (this.state.originalItems[i]) {
                this.state.originalItems[i].style.display = 'block';
            }
        }

        // Сохраняем видимые элементы
        this.state.visibleItems = this.state.originalItems.slice(startIndex, endIndex);

        // Обновляем UI
        this.updatePageInfo();
        this.updatePageNumbers();
        this.updateNavigationButtons();

        // Прокручиваем к началу списка
        const keywordContainer = document.getElementById(this.config.containerId);
        if (keywordContainer) {
            keywordContainer.scrollTop = 0;
        }

        console.log(`KeywordsPagination: Showing items ${startIndex + 1}-${endIndex} of ${this.state.totalItems}`);
    },

    // Обновить информацию о странице
    updatePageInfo() {
        const startIndex = (this.config.currentPage - 1) * this.config.itemsPerPage + 1;
        const endIndex = Math.min(this.config.currentPage * this.config.itemsPerPage, this.state.totalItems);

        const currentPageEl = document.querySelector('.keyword-current-page');
        const totalPagesEl = document.querySelector('.keyword-total-pages');
        const startIndexEl = document.querySelector('.keyword-start-index');
        const endIndexEl = document.querySelector('.keyword-end-index');
        const totalItemsEl = document.querySelector('.keyword-total-items');

        if (currentPageEl) currentPageEl.textContent = this.config.currentPage;
        if (totalPagesEl) totalPagesEl.textContent = this.state.totalPages;
        if (startIndexEl) startIndexEl.textContent = startIndex;
        if (endIndexEl) endIndexEl.textContent = endIndex;
        if (totalItemsEl) totalItemsEl.textContent = this.state.totalItems;
    },

    // Обновить номера страниц
    updatePageNumbers() {
        const pageNumbersContainer = document.querySelector('.keyword-page-numbers');
        if (!pageNumbersContainer) return;

        pageNumbersContainer.innerHTML = '';

        // Если всего 1 страница, не показываем номера
        if (this.state.totalPages <= 1) {
            return;
        }

        // Определяем диапазон видимых страниц
        let startPage = Math.max(1, this.config.currentPage - 2);
        let endPage = Math.min(this.state.totalPages, this.config.currentPage + 2);

        // Корректируем если показываем слишком мало страниц
        if (endPage - startPage < 4) {
            if (startPage === 1) {
                endPage = Math.min(this.state.totalPages, startPage + 4);
            } else if (endPage === this.state.totalPages) {
                startPage = Math.max(1, endPage - 4);
            }
        }

        // Кнопка "Первая страница" если нужно
        if (startPage > 1) {
            const firstBtn = this.createPageButton(1);
            pageNumbersContainer.appendChild(firstBtn);

            if (startPage > 2) {
                const ellipsis = document.createElement('span');
                ellipsis.className = 'text-muted mx-1';
                ellipsis.textContent = '...';
                ellipsis.style.fontWeight = 'bold';
                pageNumbersContainer.appendChild(ellipsis);
            }
        }

        // Номера страниц
        for (let i = startPage; i <= endPage; i++) {
            const pageBtn = this.createPageButton(i);
            pageNumbersContainer.appendChild(pageBtn);
        }

        // Кнопка "Последняя страница" если нужно
        if (endPage < this.state.totalPages) {
            if (endPage < this.state.totalPages - 1) {
                const ellipsis = document.createElement('span');
                ellipsis.className = 'text-muted mx-1';
                ellipsis.textContent = '...';
                ellipsis.style.fontWeight = 'bold';
                pageNumbersContainer.appendChild(ellipsis);
            }

            const lastBtn = this.createPageButton(this.state.totalPages);
            pageNumbersContainer.appendChild(lastBtn);
        }
    },

    // Создать кнопку страницы (с оранжевыми рамками)
    createPageButton(pageNumber) {
        const button = document.createElement('button');
        button.type = 'button';
        button.className = `btn btn-sm page-number-btn ${pageNumber === this.config.currentPage ? 'btn-primary' : 'btn-outline-secondary'}`;
        button.textContent = pageNumber;
        button.title = `Page ${pageNumber}`;

        // Добавляем оранжевую рамку
        button.style.border = '1px solid var(--border, rgba(255, 107, 53, 0.2))';
        button.style.borderRadius = '6px';
        button.style.padding = '0.5rem 1rem';
        button.style.fontWeight = '600';
        button.style.minWidth = '2.5rem';
        button.style.height = '2.5rem';
        button.style.transition = 'all 0.2s ease';

        // Стили для активной страницы
        if (pageNumber === this.config.currentPage) {
            button.style.background = 'linear-gradient(135deg, var(--secondary-color, #ff6b35), var(--accent-color, #ff8e53))';
            button.style.borderColor = 'var(--secondary-color, #ff6b35)';
            button.style.color = 'white';
            button.style.boxShadow = '0 4px 12px rgba(255, 107, 53, 0.3)';
        } else {
            button.style.backgroundColor = 'var(--surface, rgba(26, 26, 26, 0.4))';
            button.style.color = 'var(--text-dark, white)';
        }

        button.addEventListener('click', () => {
            this.showPage(pageNumber);
        });

        // Hover эффекты
        button.addEventListener('mouseenter', function() {
            if (!this.classList.contains('btn-primary')) {
                this.style.backgroundColor = 'rgba(255, 107, 53, 0.1)';
                this.style.borderColor = 'var(--secondary-color, #ff6b35)';
                this.style.color = 'var(--secondary-color, #ff6b35)';
                this.style.transform = 'translateY(-2px)';
                this.style.boxShadow = '0 4px 12px rgba(255, 107, 53, 0.2)';
            } else {
                this.style.transform = 'translateY(-2px)';
                this.style.boxShadow = '0 6px 20px rgba(255, 107, 53, 0.4)';
            }
        });

        button.addEventListener('mouseleave', function() {
            if (!this.classList.contains('btn-primary')) {
                this.style.backgroundColor = 'var(--surface, rgba(26, 26, 26, 0.4))';
                this.style.borderColor = 'var(--border, rgba(255, 107, 53, 0.2))';
                this.style.color = 'var(--text-dark, white)';
                this.style.transform = 'translateY(0)';
                this.style.boxShadow = 'none';
            } else {
                this.style.transform = 'translateY(0)';
                this.style.boxShadow = '0 4px 12px rgba(255, 107, 53, 0.3)';
            }
        });

        return button;
    },

    // Обновить кнопки навигации (с оранжевыми рамками)
    updateNavigationButtons() {
        const prevBtn = document.querySelector('.keyword-prev-btn');
        const nextBtn = document.querySelector('.keyword-next-btn');

        if (prevBtn) {
            const isDisabled = this.config.currentPage === 1;
            prevBtn.disabled = isDisabled;
            prevBtn.classList.toggle('disabled', isDisabled);

            // Оранжевые рамки для кнопок навигации
            prevBtn.style.border = '1px solid var(--border, rgba(255, 107, 53, 0.2))';
            prevBtn.style.borderRadius = '6px';
            prevBtn.style.fontWeight = '600';
            prevBtn.style.padding = '0.5rem 1rem';
            prevBtn.style.transition = 'all 0.2s ease';

            if (isDisabled) {
                prevBtn.style.opacity = '0.5';
                prevBtn.style.cursor = 'not-allowed';
                prevBtn.style.borderColor = 'var(--border-dark, #666)';
                prevBtn.style.color = 'var(--text-muted, #666)';
                prevBtn.style.backgroundColor = 'var(--surface-dark, rgba(18, 18, 18, 0.6))';
            } else {
                prevBtn.style.opacity = '1';
                prevBtn.style.cursor = 'pointer';
                prevBtn.style.borderColor = 'var(--secondary-color, #ff6b35)';
                prevBtn.style.color = 'var(--secondary-color, #ff6b35)';
                prevBtn.style.backgroundColor = 'var(--surface, rgba(26, 26, 26, 0.4))';

                // Hover эффект для активной кнопки Prev
                prevBtn.addEventListener('mouseenter', function() {
                    this.style.backgroundColor = 'rgba(255, 107, 53, 0.1)';
                    this.style.color = 'var(--secondary-color, #ff6b35)';
                    this.style.transform = 'translateY(-2px)';
                    this.style.boxShadow = '0 4px 12px rgba(255, 107, 53, 0.2)';
                });

                prevBtn.addEventListener('mouseleave', function() {
                    this.style.backgroundColor = 'var(--surface, rgba(26, 26, 26, 0.4))';
                    this.style.color = 'var(--secondary-color, #ff6b35)';
                    this.style.transform = 'translateY(0)';
                    this.style.boxShadow = 'none';
                });
            }
        }

        if (nextBtn) {
            const isDisabled = this.config.currentPage === this.state.totalPages;
            nextBtn.disabled = isDisabled;
            nextBtn.classList.toggle('disabled', isDisabled);

            // Оранжевые рамки для кнопок навигации
            nextBtn.style.border = '1px solid var(--border, rgba(255, 107, 53, 0.2))';
            nextBtn.style.borderRadius = '6px';
            nextBtn.style.fontWeight = '600';
            nextBtn.style.padding = '0.5rem 1rem';
            nextBtn.style.transition = 'all 0.2s ease';

            if (isDisabled) {
                nextBtn.style.opacity = '0.5';
                nextBtn.style.cursor = 'not-allowed';
                nextBtn.style.borderColor = 'var(--border-dark, #666)';
                nextBtn.style.color = 'var(--text-muted, #666)';
                nextBtn.style.backgroundColor = 'var(--surface-dark, rgba(18, 18, 18, 0.6))';
            } else {
                nextBtn.style.opacity = '1';
                nextBtn.style.cursor = 'pointer';
                nextBtn.style.borderColor = 'var(--secondary-color, #ff6b35)';
                nextBtn.style.color = 'var(--secondary-color, #ff6b35)';
                nextBtn.style.backgroundColor = 'var(--surface, rgba(26, 26, 26, 0.4))';

                // Hover эффект для активной кнопки Next
                nextBtn.addEventListener('mouseenter', function() {
                    this.style.backgroundColor = 'rgba(255, 107, 53, 0.1)';
                    this.style.color = 'var(--secondary-color, #ff6b35)';
                    this.style.transform = 'translateY(-2px)';
                    this.style.boxShadow = '0 4px 12px rgba(255, 107, 53, 0.2)';
                });

                nextBtn.addEventListener('mouseleave', function() {
                    this.style.backgroundColor = 'var(--surface, rgba(26, 26, 26, 0.4))';
                    this.style.color = 'var(--secondary-color, #ff6b35)';
                    this.style.transform = 'translateY(0)';
                    this.style.boxShadow = 'none';
                });
            }
        }
    },

    // Установить обработчики событий
    setupEventListeners() {
        console.log('KeywordsPagination: Setting up event listeners...');

        // Кнопка "Предыдущая"
        const prevBtn = document.querySelector('.keyword-prev-btn');
        if (prevBtn) {
            prevBtn.addEventListener('click', () => {
                if (this.config.currentPage > 1) {
                    this.showPage(this.config.currentPage - 1);
                }
            });
        }

        // Кнопка "Следующая"
        const nextBtn = document.querySelector('.keyword-next-btn');
        if (nextBtn) {
            nextBtn.addEventListener('click', () => {
                if (this.config.currentPage < this.state.totalPages) {
                    this.showPage(this.config.currentPage + 1);
                }
            });
        }
    },

    // Обновить после поиска
    updateAfterSearch() {
        KeywordsPaginationDebugTimer.start('KeywordsPagination.updateAfterSearch');
        console.log('KeywordsPagination: Updating after search...');

        if (!this.state.isInitialized) {
            console.log('KeywordsPagination: Not initialized yet, skipping update');
            KeywordsPaginationDebugTimer.end('KeywordsPagination.updateAfterSearch');
            return;
        }

        // Находим видимые элементы после фильтрации поиска
        const keywordContainer = document.getElementById(this.config.containerId);
        if (!keywordContainer) return;

        const visibleItems = keywordContainer.querySelectorAll('.keyword-item[style*="block"]');
        const totalVisible = visibleItems.length;

        console.log(`KeywordsPagination: ${totalVisible} visible items after search`);

        // Если элементов меньше чем на одну страницу, скрываем пагинацию
        if (totalVisible <= this.config.itemsPerPage) {
            if (this.state.paginationElement) {
                this.state.paginationElement.style.display = 'none';
            }
        } else {
            // Показываем пагинацию и пересчитываем
            if (this.state.paginationElement) {
                this.state.paginationElement.style.display = 'block';
            }

            // Временно обновляем состояние
            this.state.totalItems = totalVisible;
            this.state.totalPages = Math.ceil(totalVisible / this.config.itemsPerPage);
            this.config.currentPage = 1;

            // Обновляем UI
            this.updatePageInfo();
            this.updatePageNumbers();
            this.updateNavigationButtons();
        }
        KeywordsPaginationDebugTimer.end('KeywordsPagination.updateAfterSearch');
    },

    // Принудительное обновление (после очистки поиска)
    forceUpdate() {
        KeywordsPaginationDebugTimer.start('KeywordsPagination.forceUpdate');
        console.log('KeywordsPagination: Force updating...');

        if (!this.state.isInitialized) {
            this.init();
            KeywordsPaginationDebugTimer.end('KeywordsPagination.forceUpdate');
            return;
        }

        // Пересчитываем общее количество
        const keywordContainer = document.getElementById(this.config.containerId);
        if (!keywordContainer) return;

        const keywordItems = keywordContainer.querySelectorAll('.keyword-item');
        this.state.totalItems = keywordItems.length;
        this.state.originalItems = Array.from(keywordItems);

        // Если элементов много, показываем пагинацию
        if (this.state.totalItems > this.config.itemsPerPage) {
            this.state.totalPages = Math.ceil(this.state.totalItems / this.config.itemsPerPage);

            if (this.state.paginationElement) {
                this.state.paginationElement.style.display = 'block';
            }

            // Показываем первую страницу
            this.config.currentPage = 1;
            this.showPage(1);
        } else {
            // Если элементов мало, скрываем пагинацию и показываем все
            if (this.state.paginationElement) {
                this.state.paginationElement.style.display = 'none';
            }

            this.state.originalItems.forEach(item => {
                item.style.display = 'block';
            });
        }
        KeywordsPaginationDebugTimer.end('KeywordsPagination.forceUpdate');
    },

    // Деструктор (очистка)
    destroy() {
        console.log('KeywordsPagination: Destroying...');

        this.removeExistingPagination();
        this.state.isInitialized = false;
        this.state.originalItems = [];
        this.state.visibleItems = [];
        this.state.paginationElement = null;
        this.state.showAllButton = null;
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
    console.log('KeywordsPagination: DOM loaded, checking for initialization...');

    // Ждем немного для загрузки всех элементов
    setTimeout(() => {
        const keywordContainer = document.querySelector('.keyword-list');
        if (keywordContainer && keywordContainer.querySelectorAll('.keyword-item').length > 30) {
            console.log('KeywordsPagination: Auto-initializing...');
            KeywordsPagination.init();
        } else {
            console.log('KeywordsPagination: Not enough keywords for pagination');
        }
    }, 500);
});

export default KeywordsPagination;