// games/static/games/js/game_list/keywords-pagination.js

const KeywordsPagination = {
    init: function() {
        console.log('KeywordsPagination initializing...');

        this.initKeywordContainers();

        this.isMobile = window.innerWidth <= 768;

        window.addEventListener('resize', () => {
            const wasMobile = this.isMobile;
            this.isMobile = window.innerWidth <= 768;
            if (wasMobile !== this.isMobile) {
                this.initKeywordContainers();
            }
        });

        document.addEventListener('games-grid-updated', () => {
            setTimeout(() => this.initKeywordContainers(), 100);
        });
    },

    initKeywordContainers: function() {
        this.initContainer({
            containerId: 'search-keywords-list-container',
            inputId: 'search-keyword-search-main',
            type: 'search',
            inputName: 'keywords'
        });

        this.initContainer({
            containerId: 'similarity-keywords-list-container',
            inputId: 'similarity-keyword-search-main',
            type: 'similarity',
            inputName: 'keywords'
        });
    },

    initContainer: function(config) {
        const container = document.getElementById(config.containerId);
        if (!container) {
            console.log('Container not found:', config.containerId);
            return;
        }

        const searchInput = document.getElementById(config.inputId);
        let currentPage = 1;
        let totalPages = 1;
        let isLoading = false;
        let currentSearchTerm = '';
        let touchStartX = 0;
        let touchEndX = 0;
        let lastSwipeTime = 0;
        let isSwiping = false;
        const SWIPE_COOLDOWN = 300;
        const SWIPE_THRESHOLD = 30;

        function getSelectedIds() {
            const ids = [];
            const badgeContainer = config.type === 'search' ?
                document.getElementById('search-visible-keywords-badges') :
                document.getElementById('visible-keywords-badges');

            if (badgeContainer) {
                const badges = badgeContainer.querySelectorAll('[data-keyword-id]');
                badges.forEach(badge => {
                    const id = badge.getAttribute('data-keyword-id');
                    if (id) ids.push(id);
                });
            }
            return ids;
        }

        function addKeywordBadge(keywordId, keywordName) {
            const badgeContainer = config.type === 'search' ?
                document.getElementById('search-visible-keywords-badges') :
                document.getElementById('visible-keywords-badges');

            if (badgeContainer) {
                const existingBadge = badgeContainer.querySelector(`[data-keyword-id="${keywordId}"]`);
                if (!existingBadge) {
                    const badge = document.createElement('span');
                    badge.className = config.type === 'search' ?
                        'badge bg-success me-1 mb-1 search-active-keyword-tag' :
                        'badge bg-success me-1 mb-1 active-keyword-tag';
                    badge.setAttribute('data-keyword-id', keywordId);
                    badge.style.cssText = 'font-size: 0.85rem; padding: 0.5rem 0.75rem; border-radius: 15px; cursor: pointer;';
                    badge.innerHTML = keywordName + ' <i class="bi bi-x ms-1" style="font-size: 0.9rem;"></i>';

                    badge.addEventListener('click', function(e) {
                        e.stopPropagation();
                        const id = this.getAttribute('data-keyword-id');
                        removeKeywordBadge(id);
                        const gridDiv = container.querySelector('.keyword-grid');
                        const cb = gridDiv ? gridDiv.querySelector(`.keyword-checkbox[value="${id}"]`) : null;
                        if (cb) cb.checked = false;
                    });

                    badgeContainer.appendChild(badge);
                }
            }
        }

        function removeKeywordBadge(keywordId) {
            const badgeContainer = config.type === 'search' ?
                document.getElementById('search-visible-keywords-badges') :
                document.getElementById('visible-keywords-badges');

            if (badgeContainer) {
                const badge = badgeContainer.querySelector(`[data-keyword-id="${keywordId}"]`);
                if (badge) badge.remove();
            }
        }

        function updatePaginationDisplay(data) {
            totalPages = data.total_pages;
            currentPage = data.current_page;

            const paginationDiv = container.querySelector('.keyword-pagination-controls');

            if (paginationDiv) {
                paginationDiv.style.display = totalPages > 1 ? 'flex' : 'none';

                const currentSpan = paginationDiv.querySelector('.current-page');
                const totalSpan = paginationDiv.querySelector('.total-pages');
                const prevBtn = paginationDiv.querySelector('.keyword-prev-page');
                const nextBtn = paginationDiv.querySelector('.keyword-next-page');

                if (currentSpan) currentSpan.textContent = currentPage;
                if (totalSpan) totalSpan.textContent = totalPages;

                if (prevBtn) {
                    const newPrev = prevBtn.cloneNode(true);
                    prevBtn.parentNode.replaceChild(newPrev, prevBtn);
                    newPrev.disabled = currentPage <= 1;
                    newPrev.addEventListener('click', function(e) {
                        e.preventDefault();
                        e.stopPropagation();
                        if (currentPage > 1 && !isLoading) {
                            loadKeywordsPage(currentPage - 1, currentSearchTerm, 'prev');
                        }
                    });
                }

                if (nextBtn) {
                    const newNext = nextBtn.cloneNode(true);
                    nextBtn.parentNode.replaceChild(newNext, nextBtn);
                    newNext.disabled = currentPage >= totalPages;
                    newNext.addEventListener('click', function(e) {
                        e.preventDefault();
                        e.stopPropagation();
                        if (currentPage < totalPages && !isLoading) {
                            loadKeywordsPage(currentPage + 1, currentSearchTerm, 'next');
                        }
                    });
                }
            }
        }

        function loadKeywordsPage(page, searchTerm, direction = null) {
            if (isLoading) return;
            isLoading = true;

            const loadingDiv = container.querySelector('.keyword-loading');
            const gridDiv = container.querySelector('.keyword-grid');
            const paginationDiv = container.querySelector('.keyword-pagination-controls');

            if (loadingDiv) loadingDiv.style.display = 'block';
            if (gridDiv) {
                if (direction === 'next') {
                    gridDiv.style.animation = 'slideLeft 0.15s ease-out';
                } else if (direction === 'prev') {
                    gridDiv.style.animation = 'slideRight 0.15s ease-out';
                }
                setTimeout(() => {
                    if (gridDiv) gridDiv.style.animation = '';
                }, 150);
                gridDiv.style.display = 'none';
            }

            if (paginationDiv) paginationDiv.style.opacity = '0.5';

            const isMobile = window.innerWidth <= 768;
            let url = '/ajax/load-keywords/?page=' + page + '&type=' + config.type +
                      '&input_name=' + config.inputName + '&container_id=' + config.containerId +
                      '&mobile=' + (isMobile ? '1' : '0');

            const selectedIds = getSelectedIds();
            if (selectedIds.length > 0) {
                url += '&selected=' + selectedIds.join(',');
            }

            if (searchTerm && searchTerm.trim()) {
                url += '&search=' + encodeURIComponent(searchTerm.trim());
            }

            fetch(url, {
                headers: { 'X-Requested-With': 'XMLHttpRequest' }
            })
            .then(response => response.json())
            .then(data => {
                if (data.html && gridDiv) {
                    gridDiv.innerHTML = data.html;

                    const checkboxes = gridDiv.querySelectorAll('.keyword-checkbox');
                    checkboxes.forEach(checkbox => {
                        checkbox.addEventListener('change', function() {
                            const keywordId = this.value;
                            const label = this.nextElementSibling;
                            const keywordName = label ? label.textContent.trim() : keywordId;

                            if (this.checked) {
                                addKeywordBadge(keywordId, keywordName);
                            } else {
                                removeKeywordBadge(keywordId);
                            }
                        });
                    });
                }

                updatePaginationDisplay(data);
                currentSearchTerm = searchTerm || '';

                if (loadingDiv) loadingDiv.style.display = 'none';
                if (gridDiv) gridDiv.style.display = 'grid';
                if (paginationDiv) paginationDiv.style.opacity = '1';
                isLoading = false;
                isSwiping = false;
            })
            .catch(error => {
                console.error('Error loading keywords:', error);
                if (loadingDiv) loadingDiv.style.display = 'none';
                if (gridDiv) gridDiv.style.display = 'grid';
                if (paginationDiv) paginationDiv.style.opacity = '1';
                isLoading = false;
                isSwiping = false;
            });
        }

        function createUnifiedPagination() {
            let paginationDiv = container.querySelector('.keyword-pagination-controls');

            if (!paginationDiv) {
                paginationDiv = document.createElement('div');
                paginationDiv.className = 'keyword-pagination-controls';
                paginationDiv.innerHTML = `
                    <button class="keyword-prev-page" disabled>← Previous</button>
                    <span class="keyword-page-info">Page <span class="current-page">1</span> of <span class="total-pages">1</span></span>
                    <button class="keyword-next-page" disabled>Next →</button>
                `;
                container.appendChild(paginationDiv);
            }
        }

        function addSwipeHandlers() {
            const gridDiv = container.querySelector('.keyword-grid');
            if (!gridDiv) return;

            if (gridDiv._swipeStartHandler) {
                gridDiv.removeEventListener('touchstart', gridDiv._swipeStartHandler);
            }
            if (gridDiv._swipeEndHandler) {
                gridDiv.removeEventListener('touchend', gridDiv._swipeEndHandler);
            }

            gridDiv._swipeStartHandler = function(e) {
                if (isLoading || isSwiping) return;
                touchStartX = e.changedTouches[0].screenX;
            };

            gridDiv._swipeEndHandler = function(e) {
                if (isLoading || isSwiping) return;

                const now = Date.now();
                if (now - lastSwipeTime < SWIPE_COOLDOWN) return;

                touchEndX = e.changedTouches[0].screenX;
                const diff = touchEndX - touchStartX;

                if (Math.abs(diff) < SWIPE_THRESHOLD) return;

                e.preventDefault();
                isSwiping = true;
                lastSwipeTime = now;

                if (diff > 0 && currentPage > 1) {
                    loadKeywordsPage(currentPage - 1, currentSearchTerm, 'prev');
                } else if (diff < 0 && currentPage < totalPages) {
                    loadKeywordsPage(currentPage + 1, currentSearchTerm, 'next');
                        } else {
                    isSwiping = false;
                }
            };

            gridDiv.addEventListener('touchstart', gridDiv._swipeStartHandler, { passive: false });
            gridDiv.addEventListener('touchend', gridDiv._swipeEndHandler, { passive: false });
        }

        createUnifiedPagination();

        loadKeywordsPage(1, '');
        addSwipeHandlers();

        if (searchInput) {
            let debounceTimer;
            searchInput.addEventListener('input', function() {
                clearTimeout(debounceTimer);
                debounceTimer = setTimeout(() => {
                    loadKeywordsPage(1, this.value);
                }, 300);
            });
        }

        const observer = new MutationObserver(() => {
            addSwipeHandlers();
        });

        const gridDiv = container.querySelector('.keyword-grid');
        if (gridDiv) {
            observer.observe(gridDiv, { childList: true, subtree: true });
        }
    }
};

const style = document.createElement('style');
style.textContent = `
    @keyframes slideLeft {
        0% { transform: translateX(0); opacity: 1; }
        100% { transform: translateX(-100%); opacity: 0; }
    }
    @keyframes slideRight {
        0% { transform: translateX(0); opacity: 1; }
        100% { transform: translateX(100%); opacity: 0; }
    }

    .keyword-pagination-controls {
        display: flex;
        justify-content: center;
        align-items: center;
        gap: 16px;
        margin-top: 15px;
        padding: 12px;
        background: rgba(255, 255, 255, 0.03);
        border-radius: 40px;
        transition: opacity 0.2s ease;
    }

    .keyword-prev-page, .keyword-next-page {
        background: var(--secondary-color);
        border: none;
        color: white;
        padding: 8px 20px;
        border-radius: 30px;
        font-size: 14px;
        font-weight: 600;
        cursor: pointer;
        transition: all 0.2s ease;
        display: inline-flex;
        align-items: center;
        gap: 6px;
    }

    .keyword-prev-page:active, .keyword-next-page:active {
        transform: scale(0.95);
    }

    .keyword-prev-page:disabled, .keyword-next-page:disabled {
        opacity: 0.4;
        cursor: not-allowed;
        transform: none;
    }

    .keyword-page-info {
        font-size: 14px;
        font-weight: 600;
        color: var(--text-dark);
        background: rgba(0, 0, 0, 0.3);
        padding: 6px 14px;
        border-radius: 30px;
    }

    .keyword-page-info .current-page {
        color: var(--secondary-color);
        font-weight: 700;
        font-size: 16px;
    }

    @media (max-width: 768px) {
        .keyword-pagination-controls {
            gap: 12px;
            padding: 10px;
            position: sticky;
            bottom: 0;
            background: var(--surface-dark);
            border-top: 1px solid var(--border);
            border-radius: 0;
            margin-top: 10px;
        }

        .keyword-prev-page, .keyword-next-page {
            padding: 8px 16px;
            font-size: 13px;
        }

        .keyword-page-info {
            font-size: 13px;
            padding: 5px 12px;
        }
    }

    @media (max-width: 480px) {
        .keyword-prev-page, .keyword-next-page {
            padding: 6px 12px;
            font-size: 12px;
        }

        .keyword-page-info {
            font-size: 12px;
            padding: 4px 10px;
        }
    }
`;
document.head.appendChild(style);

document.addEventListener('DOMContentLoaded', () => {
    KeywordsPagination.init();
});

window.KeywordsPagination = KeywordsPagination;