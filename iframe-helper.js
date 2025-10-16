const scriptSrc = document.currentScript.src;
const trustedOrigin = new URL(scriptSrc).origin;
function postNavigationMessage(url) {
    if (url && typeof url === 'string' && (url.startsWith('http') || url.startsWith('/'))) {
        window.parent.postMessage({
            type: 'rivift-browser-nav',
            url: url 
        }, '*'); 
    }
}
window.addEventListener('load', () => {
    document.body.addEventListener('click', (e) => {
        const link = e.target.closest('a');

        if (link && link.href) {
            if (link.target === '_blank') {
                return;
            }

            e.preventDefault();
            e.stopPropagation();
            postNavigationMessage(link.href);
        }
    }, true);
    const observer = new MutationObserver((mutations) => {
        mutations.forEach((mutation) => {
            mutation.addedNodes.forEach((node) => {
                if (node.nodeType === 1) {
                    if (node.tagName === 'A') {
                        node.addEventListener('click', handleDynamicLink, true);
                    }
                    node.querySelectorAll('a').forEach(link => {
                        link.addEventListener('click', handleDynamicLink, true);
                    });
                }
            });
        });
    });
    observer.observe(document.body, {
        childList: true,
        subtree: true
    });
    function handleDynamicLink(e) {
        if (e.currentTarget.href) {
            if (e.currentTarget.target === '_blank') return;
            e.preventDefault();
            e.stopPropagation();
            postNavigationMessage(e.currentTarget.href);
        }
    }

}, false);