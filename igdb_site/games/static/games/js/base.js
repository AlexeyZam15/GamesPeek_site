// games/static/games/js/base.js
// Global JavaScript for GamesPeek website

// Initialize all tooltips when DOM is ready
document.addEventListener('DOMContentLoaded', function() {
    // Initialize Bootstrap tooltips
    var tooltipTriggerList = [].slice.call(document.querySelectorAll('[data-bs-toggle="tooltip"]'));
    tooltipTriggerList.map(function(tooltipTriggerEl) {
        return new bootstrap.Tooltip(tooltipTriggerEl);
    });

    // Admin button hover effects
    var adminBtn = document.querySelector('button[onclick="autoLoginAndOpenAdmin()"]');
    if (adminBtn) {
        adminBtn.addEventListener('mouseenter', function() {
            this.style.background = 'rgba(255, 107, 53, 0.2)';
            this.style.transform = 'translateY(-1px)';
            this.style.boxShadow = '0 4px 12px rgba(255, 107, 53, 0.2)';
        });

        adminBtn.addEventListener('mouseleave', function() {
            this.style.background = 'rgba(255, 107, 53, 0.1)';
            this.style.transform = 'translateY(0)';
            this.style.boxShadow = 'none';
        });
    }

    // Character counter for feedback textarea
    const feedbackTextarea = document.getElementById('feedbackMessage');
    if (feedbackTextarea) {
        feedbackTextarea.addEventListener('input', function() {
            const count = this.value.length;
            const charCountSpan = document.getElementById('feedbackCharCount');
            if (charCountSpan) {
                charCountSpan.textContent = count + ' / 5000';
                if (count > 5000) {
                    charCountSpan.classList.add('text-danger');
                } else {
                    charCountSpan.classList.remove('text-danger');
                }
            }
        });
    }
});

// Auto-login and open admin panel
function autoLoginAndOpenAdmin() {
    var currentUrl = window.location.href;
    var btn = event.target.closest('button') || event.target;
    var originalHtml = btn.innerHTML;
    btn.innerHTML = '<i class="bi bi-hourglass-split me-1"></i> Logging in...';
    btn.disabled = true;

    fetch('/admin-auto-login/', {
        method: 'GET',
        credentials: 'same-origin',
        headers: {
            'Accept': 'application/json',
            'X-Requested-With': 'XMLHttpRequest'
        }
    })
    .then(response => {
        const contentType = response.headers.get("content-type");
        if (!contentType || !contentType.includes("application/json")) {
            return response.text().then(text => {
                console.error('Received non-JSON response:', text.substring(0, 200));
                throw new Error('Expected JSON but got HTML. Check if admin-auto-login URL exists.');
            });
        }
        return response.json();
    })
    .then(data => {
        console.log('Login response:', data);
        if (data.status === 'success') {
            var adminWindow = window.open('/admin/', '_blank');
            btn.innerHTML = '<i class="bi bi-check-circle me-1"></i> Logged in!';

            function checkAdminWindowClosed() {
                if (adminWindow && !adminWindow.closed) {
                    setTimeout(checkAdminWindowClosed, 2000);
                } else {
                    setTimeout(function() {
                        window.location.reload();
                    }, 500);
                }
            }

            if (adminWindow) {
                setTimeout(checkAdminWindowClosed, 3000);
            } else {
                setTimeout(function() {
                    window.location.reload();
                }, 1000);
            }

            setTimeout(() => {
                btn.innerHTML = originalHtml;
                btn.disabled = false;
            }, 3000);
        } else {
            alert('Error: ' + data.message);
            btn.innerHTML = originalHtml;
            btn.disabled = false;
        }
    })
    .catch(error => {
        console.error('Error:', error);
        alert('Error: ' + error.message + '\nCheck console for details.');
        btn.innerHTML = originalHtml;
        btn.disabled = false;
    });
}

// Send feedback with optional email
function sendFeedback() {
    const message = document.getElementById('feedbackMessage').value.trim();
    const email = document.getElementById('feedbackEmail').value.trim();
    const sendBtn = document.querySelector('#feedbackModal .btn-primary');
    const originalText = sendBtn.innerHTML;

    if (!message) {
        alert('Please enter your feedback message');
        return;
    }

    if (message.length > 5000) {
        alert('Message is too long (maximum 5000 characters)');
        return;
    }

    sendBtn.disabled = true;
    sendBtn.innerHTML = '<i class="bi bi-hourglass-split me-1"></i>Sending...';

    // Get CSRF token from cookie
    function getCookie(name) {
        let cookieValue = null;
        if (document.cookie && document.cookie !== '') {
            const cookies = document.cookie.split(';');
            for (let i = 0; i < cookies.length; i++) {
                const cookie = cookies[i].trim();
                if (cookie.substring(0, name.length + 1) === (name + '=')) {
                    cookieValue = decodeURIComponent(cookie.substring(name.length + 1));
                    break;
                }
            }
        }
        return cookieValue;
    }

    fetch('/send-feedback/', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
            'X-CSRFToken': getCookie('csrftoken'),
        },
        body: JSON.stringify({
            message: message,
            email: email
        })
    })
    .then(response => response.json())
    .then(data => {
        if (data.status === 'success') {
            document.getElementById('feedbackMessage').value = '';
            document.getElementById('feedbackEmail').value = '';
            const charCountSpan = document.getElementById('feedbackCharCount');
            if (charCountSpan) {
                charCountSpan.textContent = '0 / 5000';
            }
            const modal = bootstrap.Modal.getInstance(document.getElementById('feedbackModal'));
            if (modal) {
                modal.hide();
            }
            alert('✓ ' + data.message);
        } else {
            alert('✗ ' + data.message);
        }
    })
    .catch(error => {
        console.error('Error:', error);
        alert('✗ Failed to send feedback. Please try again later.');
    })
    .finally(() => {
        sendBtn.disabled = false;
        sendBtn.innerHTML = originalText;
    });
}