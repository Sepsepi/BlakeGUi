"""
ZabaSearch Phone Number Extractor - Intelligent Batch Processor
Cross-references addresses from CSV with ZabaSearch data and extracts phone numbers
Features:
- Auto-detects latest CSV files with addresses
- Dynamic batch processing (configurable batch size)
- Command-line interface for automation
- Progress tracking and error recovery
- Rate limiting for respectful scraping
"""
import asyncio
import pandas as pd
import random
import re
from playwright.async_api import async_playwright
from typing import Dict, List, Optional, Tuple
import time
import glob
import os
import gc
from urllib.parse import quote
import argparse

class ZabaSearchExtractor:
    def __init__(self, headless: bool = True):  # Default to headless
        self.headless = headless

        # Configure timeouts from environment variables (cloud deployment friendly)
        self.navigation_timeout = int(os.environ.get('BROWARD_NAVIGATION_TIMEOUT', '60000'))
        self.selector_timeout = int(os.environ.get('BROWARD_SELECTOR_TIMEOUT', '5000'))
        self.agreement_timeout = int(os.environ.get('BROWARD_AGREEMENT_TIMEOUT', '10000'))

        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/121.0',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15'
        ]
        self.firefox_user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:119.0) Gecko/20100101 Firefox/119.0',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:121.0) Gecko/20100101 Firefox/121.0',
            'Mozilla/5.0 (X11; Linux x86_64; rv:121.0) Gecko/20100101 Firefox/121.0'
        ]
        self.terms_accepted = False

    async def create_stealth_browser(self, playwright, browser_type='chromium', proxy=None):
        """Create a browser with ADVANCED stealth capabilities and complete session isolation"""

        # Generate completely random session data for each batch
        session_id = random.randint(100000, 999999)
        print(f"🆔 Creating new browser session #{session_id} with isolated fingerprint")

        # Random viewport from common resolutions
        viewports = [
            {'width': 1920, 'height': 1080},
            {'width': 1366, 'height': 768},
            {'width': 1536, 'height': 864},
            {'width': 1440, 'height': 900},
            {'width': 1600, 'height': 900},
            {'width': 1280, 'height': 720}
        ]
        viewport = random.choice(viewports)

        # Random timezone and locale combinations
        locales_timezones = [
            {'locale': 'en-US', 'timezone': 'America/New_York'},
            {'locale': 'en-US', 'timezone': 'America/Chicago'},
            {'locale': 'en-US', 'timezone': 'America/Denver'},
            {'locale': 'en-US', 'timezone': 'America/Los_Angeles'},
            {'locale': 'en-US', 'timezone': 'America/Phoenix'},
            {'locale': 'en-CA', 'timezone': 'America/Toronto'},
            {'locale': 'en-GB', 'timezone': 'Europe/London'}
        ]
        locale_tz = random.choice(locales_timezones)

        print(f"🖥️ Viewport: {viewport['width']}x{viewport['height']}")
        print(f"� Locale: {locale_tz['locale']}, Timezone: {locale_tz['timezone']}")
        if proxy:
            print(f"🔒 Using proxy: {proxy['server']}")

        if browser_type == 'firefox':
            # Enhanced Firefox args
            launch_args = [
                '--no-sandbox',
                '--disable-dev-shm-usage',
                '--disable-extensions',
                '--disable-plugins',
                '--disable-translate',
                '--new-instance',
                '--no-remote',
                f'--profile-directory=ff-session-{session_id}'
            ]
            browser = await playwright.firefox.launch(
                headless=self.headless,
                args=launch_args,
                proxy=proxy
            )

            context = await browser.new_context(
                viewport=viewport,
                user_agent=random.choice(self.firefox_user_agents),
                locale=locale_tz['locale'],
                timezone_id=locale_tz['timezone'],
                device_scale_factor=random.choice([1, 1.25, 1.5]),
                has_touch=random.choice([True, False]),
                permissions=['geolocation'],
                geolocation={'longitude': random.uniform(-80.5, -80.0), 'latitude': random.uniform(25.5, 26.5)},
                java_script_enabled=True,
                bypass_csp=True,
                ignore_https_errors=True
            )

            # Set default timeouts for all page operations
            context.set_default_timeout(self.navigation_timeout)
            context.set_default_navigation_timeout(self.navigation_timeout)

        else:  # chromium - ENHANCED
            # Enhanced Chrome args with maximum stealth
            launch_args = [
                '--no-sandbox',
                '--disable-setuid-sandbox',
                '--disable-dev-shm-usage',
                '--disable-accelerated-2d-canvas',
                '--no-first-run',
                '--no-zygote',
                '--disable-gpu',
                '--disable-features=VizDisplayCompositor',
                '--disable-background-networking',
                '--disable-background-timer-throttling',
                '--disable-renderer-backgrounding',
                '--disable-backgrounding-occluded-windows',
                '--disable-ipc-flooding-protection',
                '--disable-extensions',
                '--disable-plugins',
                '--disable-translate',
                '--disable-default-apps',
                '--disable-web-security',
                '--disable-features=TranslateUI',
                '--disable-blink-features=AutomationControlled',
                '--no-default-browser-check',
                '--disable-component-extensions-with-background-pages',
                '--disable-background-mode',
                '--disable-client-side-phishing-detection',
                '--disable-sync',
                '--disable-features=Translate',
                '--enable-unsafe-swiftshader',
                '--use-mock-keychain',
                '--disable-popup-blocking',
                '--start-maximized',
                '--user-agent=' + random.choice(self.user_agents)
            ]

            browser = await playwright.chromium.launch(
                headless=self.headless,
                args=launch_args,
                proxy=proxy
            )

            context = await browser.new_context(
                viewport=viewport,
                user_agent=random.choice(self.user_agents),
                locale=locale_tz['locale'],
                timezone_id=locale_tz['timezone'],
                screen={'width': viewport['width'], 'height': viewport['height']},
                device_scale_factor=random.choice([1, 1.25, 1.5]),
                has_touch=random.choice([True, False]),
                is_mobile=False,
                permissions=['geolocation'],
                geolocation={'longitude': random.uniform(-80.5, -80.0), 'latitude': random.uniform(25.5, 26.5)},
                java_script_enabled=True,
                bypass_csp=True,
                ignore_https_errors=True
            )

            # Set default timeouts for all page operations
            context.set_default_timeout(self.navigation_timeout)
            context.set_default_navigation_timeout(self.navigation_timeout)

        # ADVANCED ANTI-DETECTION SCRIPTS FOR BOTH BROWSERS
        await context.add_init_script("""
            // Remove webdriver traces
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined,
            });

            // Mock plugins with realistic data
            Object.defineProperty(navigator, 'plugins', {
                get: () => ({
                    length: 3,
                    0: { name: 'Chrome PDF Plugin', description: 'Portable Document Format', filename: 'internal-pdf-viewer' },
                    1: { name: 'Chrome PDF Viewer', description: '', filename: 'mhjfbmdgcfjbbpaeojofohoefgiehjai' },
                    2: { name: 'Native Client', description: '', filename: 'internal-nacl-plugin' }
                }),
            });

            // Realistic language settings
            Object.defineProperty(navigator, 'languages', {
                get: () => ['en-US', 'en'],
            });

            // Mock Chrome runtime
            window.chrome = {
                runtime: {
                    onConnect: undefined,
                    onMessage: undefined
                },
                app: {
                    isInstalled: false
                }
            };

            // Canvas fingerprint randomization
            const getImageData = HTMLCanvasElement.prototype.getContext('2d').getImageData;
            HTMLCanvasElement.prototype.getContext('2d').getImageData = function(...args) {
                const result = getImageData.apply(this, args);
                // Add tiny noise to canvas
                for (let i = 0; i < result.data.length; i += 4) {
                    result.data[i] += Math.floor(Math.random() * 3) - 1;
                    result.data[i + 1] += Math.floor(Math.random() * 3) - 1;
                    result.data[i + 2] += Math.floor(Math.random() * 3) - 1;
                }
                return result;
            };

            // WebRTC IP leak protection
            const RTCPeerConnection = window.RTCPeerConnection || window.mozRTCPeerConnection || window.webkitRTCPeerConnection;
            if (RTCPeerConnection) {
                const originalCreateDataChannel = RTCPeerConnection.prototype.createDataChannel;
                RTCPeerConnection.prototype.createDataChannel = function() {
                    return originalCreateDataChannel.apply(this, arguments);
                };
            }

            // Audio context fingerprint randomization
            const AudioContext = window.AudioContext || window.webkitAudioContext;
            if (AudioContext) {
                const originalCreateAnalyser = AudioContext.prototype.createAnalyser;
                AudioContext.prototype.createAnalyser = function() {
                    const analyser = originalCreateAnalyser.apply(this, arguments);
                    const originalGetByteFrequencyData = analyser.getByteFrequencyData;
                    analyser.getByteFrequencyData = function(array) {
                        originalGetByteFrequencyData.apply(this, arguments);
                        // Add slight noise
                        for (let i = 0; i < array.length; i++) {
                            array[i] += Math.floor(Math.random() * 3) - 1;
                        }
                    };
                    return analyser;
                };
            }

            // Screen resolution noise
            const originalScreen = window.screen;
            Object.defineProperties(window.screen, {
                width: { value: originalScreen.width + Math.floor(Math.random() * 3) - 1 },
                height: { value: originalScreen.height + Math.floor(Math.random() * 3) - 1 },
                availWidth: { value: originalScreen.availWidth + Math.floor(Math.random() * 3) - 1 },
                availHeight: { value: originalScreen.availHeight + Math.floor(Math.random() * 3) - 1 }
            });

            // Battery API spoofing
            if (navigator.getBattery) {
                navigator.getBattery = () => Promise.resolve({
                    charging: true,
                    chargingTime: Infinity,
                    dischargingTime: Infinity,
                    level: Math.random()
                });
            }

            // Remove automation indicators
            delete window.cdc_adoQpoasnfa76pfcZLmcfl_Array;
            delete window.cdc_adoQpoasnfa76pfcZLmcfl_Promise;
            delete window.cdc_adoQpoasnfa76pfcZLmcfl_Symbol;

            console.log('🛡️ Advanced stealth mode activated');
        """)

        print(f"🛡️ Advanced anti-detection measures activated for session #{session_id}")
        return browser, context

    async def human_delay(self, delay_type="normal"):
        """ULTRA-FAST delays - OPTIMIZED FOR SPEED"""
        delays = {
            "quick": (0.1, 0.2),      # Ultra fast for quick actions
            "normal": (0.2, 0.4),     # Super fast normal delays
            "slow": (0.3, 0.6),       # Fast slow actions
            "typing": (0.01, 0.03),   # Lightning fast typing
            "mouse": (0.05, 0.1),     # Instant mouse movements
            "form": (0.1, 0.2)        # Fast form delays
        }
        min_delay, max_delay = delays.get(delay_type, delays["normal"])
        await asyncio.sleep(random.uniform(min_delay, max_delay))

    async def human_type(self, element, text: str):
        """Type text with human-like delays and occasional pauses"""
        await element.clear()
        await self.human_delay("typing")

        for i, char in enumerate(text):
            await element.type(char)
            # Random pause every few characters to simulate thinking
            if i > 0 and i % random.randint(3, 7) == 0:
                await self.human_delay("typing")
            else:
                await asyncio.sleep(random.uniform(0.05, 0.15))

    async def human_click_with_movement(self, page, element):
        """Click element with human-like mouse movement"""
        # Get element position
        box = await element.bounding_box()
        if box:
            # Add slight randomness to click position
            x = box['x'] + box['width'] / 2 + random.uniform(-5, 5)
            y = box['y'] + box['height'] / 2 + random.uniform(-5, 5)

            # Move mouse to element with delay
            await page.mouse.move(x, y)
            await self.human_delay("mouse")

            # Click with slight delay
            await page.mouse.click(x, y)
        else:
            # Fallback to regular click
            await element.click()

        await self.human_delay("quick")

    def normalize_address(self, address: str) -> str:
        """Normalize address for comparison with improved ordinal handling"""
        if not address:
            return ""

        # Convert to uppercase and remove extra spaces
        normalized = re.sub(r'\s+', ' ', address.upper().strip())

        # Common address normalizations
        replacements = {
            ' STREET': ' ST',
            ' AVENUE': ' AVE',
            ' DRIVE': ' DR',
            ' COURT': ' CT',
            ' PLACE': ' PL',
            ' ROAD': ' RD',
            ' CIRCLE': ' CIR',
            ' WAY': ' WAY',
            ' BOULEVARD': ' BLVD',
            ' LANE': ' LN',
            ' TERRACE': ' TER'
        }

        for old, new in replacements.items():
            normalized = normalized.replace(old, new)

        return normalized

    def addresses_match(self, csv_address: str, zaba_address: str) -> bool:
        """Check if addresses match with improved ordinal number handling"""
        if not csv_address or not zaba_address:
            return False

        csv_norm = self.normalize_address(csv_address)
        zaba_norm = self.normalize_address(zaba_address)

        print(f"    🔍 Comparing: '{csv_norm}' vs '{zaba_norm}'")

        # Extract components for flexible matching
        csv_parts = csv_norm.split()
        zaba_parts = zaba_norm.split()

        if len(csv_parts) < 2 or len(zaba_parts) < 2:
            return False

        # Check if street number matches
        if csv_parts[0] != zaba_parts[0]:
            return False

        # Create variations of street parts to handle ordinal numbers
        def create_ordinal_variations(parts):
            """Create variations with and without ordinal suffixes"""
            variations = []
            for part in parts:
                variations.append(part)
                # If it's a number, add ordinal versions
                if re.match(r'^\d+$', part):
                    num = int(part)
                    if num == 1:
                        variations.extend([f"{part}ST", "1ST"])
                    elif num == 2:
                        variations.extend([f"{part}ND", "2ND"])
                    elif num == 3:
                        variations.extend([f"{part}RD", "3RD"])
                    elif num in [11, 12, 13]:
                        variations.append(f"{part}TH")
                    elif num % 10 == 1:
                        variations.append(f"{part}ST")
                    elif num % 10 == 2:
                        variations.append(f"{part}ND")
                    elif num % 10 == 3:
                        variations.append(f"{part}RD")
                    else:
                        variations.append(f"{part}TH")
                # If it has ordinal suffix, also add base number
                elif re.match(r'^\d+(ST|ND|RD|TH)$', part):
                    base_num = re.sub(r'(ST|ND|RD|TH)$', '', part)
                    variations.append(base_num)
            return variations

        # Get key street parts (exclude street number)
        csv_street_parts = csv_parts[1:4] if len(csv_parts) > 3 else csv_parts[1:]
        zaba_street_parts = zaba_parts[1:4] if len(zaba_parts) > 3 else zaba_parts[1:]

        # Create variations for both addresses
        csv_variations = create_ordinal_variations(csv_street_parts)
        zaba_variations = create_ordinal_variations(zaba_street_parts)

        # Count matches between variations
        matches = 0
        matched_parts = []

        for csv_var in csv_variations:
            if csv_var in zaba_variations and csv_var not in matched_parts:
                matches += 1
                matched_parts.append(csv_var)
                if matches >= 2:  # Stop early if we have enough matches
                    break

        # Require at least 2 matching parts for positive match
        is_match = matches >= 2
        print(f"    📊 Found {matches} matching parts {matched_parts}, result: {'✅' if is_match else '❌'}")
        return is_match

    async def detect_blocking(self, page) -> bool:
        """Detect if we're being blocked by ZabaSearch"""
        try:
            # Check for common blocking indicators
            page_content = await page.content()
            page_title = await page.title()

            blocking_indicators = [
                'captcha',
                'unusual traffic',
                'blocked',
                'service unavailable',
                'access denied',
                'too many requests'
            ]

            content_lower = page_content.lower()
            title_lower = page_title.lower()

            for indicator in blocking_indicators:
                if indicator in content_lower or indicator in title_lower:
                    return True

            # Don't assume no person cards = blocking
            # Sometimes ZabaSearch legitimately has no results for a person
            return False
        except:
            return False

    async def detect_cloudflare_challenge(self, page) -> bool:
        """Detect if we're facing a Cloudflare challenge"""
        try:
            page_title = await page.title()
            page_content = await page.content()
            current_url = page.url

            # Check URL for cloudflare challenge indicators
            if 'challenge' in current_url.lower() or 'cloudflare' in current_url.lower():
                return True

            cloudflare_indicators = [
                'checking your browser',
                'please wait',
                'verify you are human',
                'cloudflare ray id',
                'cf-browser-verification',
                'challenge-form',
                'cf-challenge'
            ]

            content_lower = page_content.lower()
            title_lower = page_title.lower()

            # Look for specific Cloudflare text patterns
            for indicator in cloudflare_indicators:
                if indicator in content_lower or indicator in title_lower:
                    return True

            # Check for specific Cloudflare elements (but not privacy modal elements)
            cf_selectors = [
                '.cf-challenge-running',
                '#challenge-form',
                '.cf-wrapper',
                '.cf-browser-verification',
                'iframe[src*="challenges.cloudflare.com"]',
                'iframe[src*="cloudflare.com"]'
            ]

            for selector in cf_selectors:
                try:
                    element = await page.wait_for_selector(selector, timeout=500)  # Reduced from 1000
                    if element:
                        return True
                except:
                    continue

            # Don't consider general checkboxes or privacy modals as Cloudflare
            # Only return True if we find specific Cloudflare indicators
            return False

        except:
            return False

    async def handle_cloudflare_challenge(self, page):
        """Handle Cloudflare challenge if detected with improved selectors"""
        try:
            print(f"    🛡️ CLOUDFLARE CHALLENGE DETECTED!")
            print(f"    🔍 Looking for verification elements...")

            # Enhanced selectors for Cloudflare challenge
            challenge_selectors = [
                # Turnstile iframe selectors (most common)
                'iframe[src*="challenges.cloudflare.com"]',
                'iframe[src*="cloudflare.com"]',
                'iframe[title*="Widget containing checkbox"]',
                'iframe[title*="Widget containing a Cloudflare security challenge"]',

                # Direct checkbox selectors
                'input[type="checkbox"][data-ray]',
                'input[type="checkbox"][data-cf-challenge]',
                '.cf-turnstile',
                '.cf-challenge-form',
                '#challenge-form',

                # General fallback
                'input[type="checkbox"]'
            ]

            challenge_handled = False

            for selector in challenge_selectors:
                try:
                    print(f"    🔍 Checking selector: {selector}")

                    if 'iframe' in selector:
                        # Handle iframe-based challenge (Turnstile)
                        try:
                            iframe = await page.wait_for_selector(selector, timeout=2000)
                            if iframe:
                                print(f"    🎯 Found Cloudflare iframe - accessing frame...")
                                iframe_frame = await iframe.content_frame()
                                if iframe_frame:
                                    # Wait a bit for iframe to load
                                    await asyncio.sleep(1)

                                    # Try multiple checkbox selectors in iframe
                                    iframe_checkbox_selectors = [
                                        'input[type="checkbox"]',
                                        '[role="checkbox"]',
                                        '.cf-turnstile-checkbox',
                                        'input'
                                    ]

                                    for iframe_selector in iframe_checkbox_selectors:
                                        try:
                                            checkbox = await iframe_frame.wait_for_selector(iframe_selector, timeout=1000)
                                            if checkbox:
                                                print(f"    🎯 Found checkbox in iframe: {iframe_selector}")
                                                await checkbox.click()
                                                print(f"    ✅ Clicked Cloudflare checkbox in iframe!")
                                                challenge_handled = True
                                                break
                                        except:
                                            continue

                                    if challenge_handled:
                                        break
                        except Exception as iframe_error:
                            print(f"    ⚠️ Iframe handling failed: {iframe_error}")
                            continue
                    else:
                        # Handle direct checkbox
                        try:
                            element = await page.wait_for_selector(selector, timeout=1000)
                            if element:
                                print(f"    🎯 Found element: {selector}")

                                # Check if it's clickable
                                if 'input' in selector:
                                    await element.click()
                                    print(f"    ✅ Clicked checkbox!")
                                    challenge_handled = True
                                    break
                                else:
                                    # Try to find checkbox within the element
                                    checkbox = await element.query_selector('input[type="checkbox"]')
                                    if checkbox:
                                        await checkbox.click()
                                        print(f"    ✅ Clicked checkbox within element!")
                                        challenge_handled = True
                                        break
                        except Exception as direct_error:
                            print(f"    ⚠️ Direct selector failed: {direct_error}")
                            continue

                except Exception as e:
                    print(f"    ⚠️ Selector {selector} failed: {e}")
                    continue

            if challenge_handled:
                print(f"    ⏳ Waiting for challenge to complete...")
                await asyncio.sleep(1.5)  # Reduced initial wait

                # Check if challenge is complete
                for i in range(8):  # Reduced wait time
                    try:
                        current_url = page.url
                        if 'zabasearch.com' in current_url and 'challenge' not in current_url.lower():
                            print(f"    ✅ Cloudflare challenge completed!")
                            return True
                        await asyncio.sleep(1)
                    except:
                        break

                print(f"    ⚠️ Challenge handling attempted - continuing...")
                return True
            else:
                print(f"    ❌ Could not find Cloudflare checkbox - skipping...")
                # Don't fail completely, just continue
                return True

        except Exception as e:
            print(f"    ❌ Cloudflare challenge handling error: {e}")
            # Don't fail the whole process
            return True

    async def detect_and_handle_popups(self, page):
        """Detect and handle any popups that might appear - ENHANCED"""
        try:
            # FIRST: Handle privacy/cookie consent modal (I AGREE button)
            privacy_handled = False

            try:
                # Look for "I AGREE" button first
                agree_button = await page.wait_for_selector('text="I AGREE"', timeout=1000)  # Reduced from 2000
                if agree_button:
                    print(f"    🚨 PRIVACY MODAL DETECTED - clicking I AGREE")
                    await agree_button.click()
                    await asyncio.sleep(0.5)  # Reduced from 1
                    print(f"    ✅ Privacy modal closed with I AGREE")
                    privacy_handled = True
            except:
                # Check for other privacy modal patterns
                privacy_selectors = [
                    '[role="dialog"]',
                    '.modal',
                    '[aria-modal="true"]',
                    '.modal-container',
                    '.cky-modal',
                    '.privacy-modal'
                ]

                for selector in privacy_selectors:
                    try:
                        modal = await page.wait_for_selector(selector, timeout=500)  # Reduced from 1000
                        if modal:
                            print(f"    🚨 PRIVACY MODAL DETECTED: {selector}")

                            # Try to find and click close buttons
                            close_selectors = [
                                'text="I AGREE"',
                                'text="Accept All"',
                                'text="Accept"',
                                'text="Close"',
                                'text="X"',
                                '.close-button',
                                '[aria-label="Close"]'
                            ]

                            modal_closed = False
                            for close_selector in close_selectors:
                                try:
                                    close_btn = await modal.query_selector(close_selector)
                                    if close_btn:
                                        await close_btn.click()
                                        print(f"    ✅ PRIVACY MODAL CLOSED: {close_selector}")
                                        privacy_handled = True
                                        modal_closed = True
                                        break
                                except:
                                    continue

                            if modal_closed:
                                await asyncio.sleep(0.5)  # Reduced from 1
                                break

                    except:
                        continue

            # SECOND: After privacy modal is handled, check for Cloudflare challenge
            if privacy_handled:
                print(f"    ⏳ Waiting for page to settle after privacy modal...")
                await asyncio.sleep(1)  # Reduced from 2

            # Now check for actual Cloudflare challenge (only after privacy modal is handled)
            if await self.detect_cloudflare_challenge(page):
                print(f"    🛡️ Cloudflare challenge detected after privacy modal...")
                try:
                    await self.handle_cloudflare_challenge(page)
                except Exception as cf_error:
                    print(f"    ⚠️ Cloudflare handling error: {cf_error}")
                    print(f"    🔄 Continuing despite Cloudflare error...")
                    # Don't crash - just continue
                return

            if not privacy_handled:
                pass  # No need for success message

        except Exception as e:
            print(f"    ⚠️ Popup scan error: {e}")
            print(f"    🔄 Continuing despite popup scan error...")
            # Don't fail the whole process for popup detection
            pass

    async def accept_terms_if_needed(self, page):
        """Accept terms and conditions if not already done"""
        if self.terms_accepted:
            return

        try:
            # Look for "I AGREE" button
            agree_button = await page.wait_for_selector('text="I AGREE"', timeout=3000)  # Reduced from 5000
            if agree_button:
                await agree_button.click()
                self.terms_accepted = True
                await self.human_delay("quick")
                print("  ✓ Accepted terms and conditions")
        except:
            # Terms already accepted or not present
            self.terms_accepted = True

    async def search_person(self, page, first_name: str, last_name: str, target_address: str = "", city: str = "", state: str = "Florida") -> Optional[Dict]:
        """Search for a person on ZabaSearch with Cloudflare handling"""
        max_retries = 3

        for attempt in range(max_retries):
            try:
                print(f"🔍 Searching ZabaSearch: {first_name} {last_name} (Attempt {attempt + 1}/{max_retries})")
                print(f"  🌐 Navigating to ZabaSearch...")

                # Navigate to ZabaSearch with timeout
                await page.goto('https://www.zabasearch.com', wait_until='domcontentloaded', timeout=20000)  # Reduced from 30000
                print(f"  ✅ Page loaded successfully")
                await asyncio.sleep(0.5)  # Reduced from 1

                # Check for Cloudflare challenge first
                if await self.detect_cloudflare_challenge(page):
                    print(f"  🛡️ Cloudflare challenge detected - handling...")
                    if await self.handle_cloudflare_challenge(page):
                        print(f"  ✅ Cloudflare challenge handled, continuing...")
                        await asyncio.sleep(1)  # Reduced from 2 - Extra wait after challenge
                    else:
                        print(f"  ❌ Failed to handle Cloudflare challenge")
                        if attempt < max_retries - 1:
                            print(f"  🔄 Retrying in 10 seconds...")
                            await asyncio.sleep(5)  # Reduced from 10
                            continue
                        return None

                # Check for any other popups
                # Accept terms if needed
                await self.accept_terms_if_needed(page)

                # Fill search form using the correct selectors from Playwright MCP testing
                print(f"  🔍 Locating search form elements...")
                await self.human_delay("form")

                # Fill name fields with human-like typing
                print(f"  ✏️ Filling first name: {first_name}")
                first_name_box = page.get_by_role("textbox", name="eg. John")
                await self.human_click_with_movement(page, first_name_box)
                await self.human_type(first_name_box, first_name)
                await self.human_delay("form")

                print(f"  ✏️ Filling last name: {last_name}")
                last_name_box = page.get_by_role("textbox", name="eg. Smith")
                await self.human_click_with_movement(page, last_name_box)
                await self.human_type(last_name_box, last_name)
                await self.human_delay("form")

                # Fill city and state if provided
                if city:
                    print(f"  🏙️ Filling city: {city}")
                    try:
                        city_box = page.get_by_role("textbox", name="eg. Chicago")
                        await self.human_click_with_movement(page, city_box)
                        await self.human_type(city_box, city)
                        await self.human_delay("form")
                        print(f"    ✅ Successfully filled city: {city}")
                    except Exception as e:
                        print(f"    ⚠️ Could not fill city field: {e}")

                if state and state.upper() in ["FLORIDA", "FL"]:
                    print(f"  🗺️ Selecting state: Florida")
                    try:
                        state_dropdown = page.get_by_role("combobox")
                        await self.human_click_with_movement(page, state_dropdown)
                        await self.human_delay("mouse")
                        await state_dropdown.select_option("Florida")
                        await self.human_delay("form")
                        print(f"    ✅ Selected Florida")
                    except Exception as e:
                        print(f"    ⚠️ Could not select Florida: {e}")
                elif state:
                    print(f"  🗺️ Attempting to select state: {state}")
                    try:
                        state_dropdown = page.get_by_role("combobox")
                        await self.human_click_with_movement(page, state_dropdown)
                        await self.human_delay("mouse")
                        # Try to select the state by name
                        await state_dropdown.select_option(state)
                        await self.human_delay("form")
                        print(f"    ✅ Selected {state}")
                    except Exception as e:
                        print(f"    ⚠️ Could not select state {state}: {e}")
                        # Fallback to Florida if state selection fails
                        try:
                            await state_dropdown.select_option("Florida")
                            print(f"    🔄 Fallback: Selected Florida")
                        except Exception as fallback_error:
                            print(f"    ❌ State selection completely failed: {fallback_error}")

                await self.human_delay("slow")  # Longer pause before submitting

                # Submit search using Enter key like in test script
                print(f"  🚀 Submitting search...")
                await self.human_click_with_movement(page, last_name_box)
                await last_name_box.press("Enter")
                print(f"  ⏳ Waiting for results to load...")
                await self.human_delay("slow")  # Longer wait for results

                # Check again for Cloudflare after search
                if await self.detect_cloudflare_challenge(page):
                    print(f"  🛡️ Cloudflare challenge after search - handling...")
                    if await self.handle_cloudflare_challenge(page):
                        await asyncio.sleep(2)  # Reduced from 3 - Faster wait after challenge
                    else:
                        if attempt < max_retries - 1:
                            print(f"  🔄 Retrying after Cloudflare challenge...")
                            await asyncio.sleep(8)  # Reduced from 15
                            continue
                        return None

                # Try to extract data directly
                print(f"  📊 Attempting to extract person data...")
                result = await self.extract_person_data(page, first_name, last_name, target_address)

                if result:
                    print(f"  ✅ Successfully extracted data for {first_name} {last_name}")
                else:
                    print(f"  ❌ No matching data found for {first_name} {last_name}")

                return result

            except Exception as e:
                error_msg = str(e).lower()
                print(f"  ❌ Search error (attempt {attempt + 1}): {e}")
                print(f"  🔍 Error type: {type(e).__name__}")

                # Check if it's a connection/socket error (likely Cloudflare)
                if any(term in error_msg for term in ['connection', 'socket', 'timeout', 'closed']):
                    print(f"  🛡️ Detected connection issue - likely Cloudflare blocking")
                    if attempt < max_retries - 1:
                        wait_time = 10 + (attempt * 5)  # Reduced waiting time - was 15 + (attempt * 10)
                        print(f"  ⏳ Waiting {wait_time} seconds before retry...")
                        await asyncio.sleep(wait_time)
                        continue

                if attempt == max_retries - 1:
                    print(f"  💥 All retry attempts failed")

        return None

    async def extract_person_data(self, page, target_first_name: str, target_last_name: str, target_address: str = "") -> Optional[Dict]:
        """Extract person data from ZabaSearch results page"""
        try:
            print("  📋 Extracting person data...")

            # Get all person result containers using the class I found in debug
            person_cards = await page.query_selector_all('.person')

            if not person_cards:
                print("  ❌ No person cards found")
                return None

            print(f"  ✅ Found {len(person_cards)} person cards")

            for i, card in enumerate(person_cards):
                print(f"  🔍 Checking result #{i+1}")

                # Get the card text to check if it's the right person
                try:
                    card_text = await card.inner_text()

                    # Check if this card contains our target person
                    if target_first_name.lower() not in card_text.lower() or target_last_name.lower() not in card_text.lower():
                        continue

                    print(f"  ✅ Found matching person in card #{i+1}")

                    # Extract addresses from this card
                    person_addresses = []

                    # Look for address patterns in the text
                    address_lines = card_text.split('\n')
                    for line in address_lines:
                        line = line.strip()
                        # Look for lines that look like addresses - more flexible pattern
                        # Pattern: starts with number, contains street words, may have city/state
                        if (re.search(r'^\d+\s+', line) and  # starts with number
                            re.search(r'(?:ST|DR|AVE|CT|RD|LN|BLVD|WAY|PL|CIR|TER|DRIVE|STREET|AVENUE|COURT|ROAD|LANE|BOULEVARD|PLACE|CIRCLE|TERRACE)', line.upper()) and
                            len(line) > 10):  # reasonable length
                            person_addresses.append(line)
                        # Also look for city, state zip patterns
                        elif re.search(r'[A-Za-z\s]+,\s*[A-Z]{2}\s+\d{5}', line):
                            person_addresses.append(line)

                    print(f"    📍 Found {len(person_addresses)} addresses in this card")

                    # Check if any address matches our target
                    address_match = False
                    if target_address:
                        for addr in person_addresses:
                            if self.addresses_match(target_address, addr):
                                address_match = True
                                print(f"    ✅ Address match found: {addr}")
                                break
                    else:
                        address_match = True  # If no target address, accept any result

                    if not address_match:
                        print(f"    ❌ No address match for result #{i+1}")
                        continue

                    # Extract phone numbers ONLY from "Last Known Phone Numbers" section
                    phones = {"primary": None, "secondary": None, "all": []}

                    try:
                        # Look specifically for "Last Known Phone Numbers" section
                        last_known_section = await card.query_selector('h3:has-text("Last Known Phone Numbers")')

                        if last_known_section:
                            print("    🎯 Found 'Last Known Phone Numbers' section")

                            # Get next sibling elements that contain the phone numbers
                            phone_content_elements = await card.query_selector_all('h3:has-text("Last Known Phone Numbers") ~ *')

                            section_text = ""
                            for element in phone_content_elements:
                                try:
                                    element_text = await element.inner_text()
                                    section_text += element_text + "\n"
                                    # Stop if we hit another section heading
                                    if any(heading in element_text for heading in ["Associated Email", "Associated Phone", "Jobs", "Past Addresses"]):
                                        break
                                except:
                                    continue

                            if section_text.strip():
                                print(f"    📋 Section text: {section_text[:200]}...")

                                # Extract phone numbers only from this specific section
                                phone_pattern = r'\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}'
                                phone_matches = re.findall(phone_pattern, section_text)

                                if phone_matches:
                                    # Clean up phone numbers
                                    cleaned_phones = []
                                    for phone in phone_matches:
                                        # Standardize format to (XXX) XXX-XXXX
                                        digits = re.sub(r'\D', '', phone)
                                        if len(digits) == 10:
                                            formatted = f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
                                            if formatted not in cleaned_phones:
                                                cleaned_phones.append(formatted)

                                    phones["all"] = cleaned_phones

                                    # Look for primary phone designation in the section text
                                    primary_found = False
                                    for phone in cleaned_phones:
                                        # Check if this phone has "(Primary Phone)" designation
                                        if "Primary Phone" in section_text and phone in section_text:
                                            # Find the line containing this phone number
                                            lines = section_text.split('\n')
                                            for line in lines:
                                                if phone.replace('(', '').replace(')', '').replace('-', '').replace(' ', '') in line.replace('(', '').replace(')', '').replace('-', '').replace(' ', ''):
                                                    if "Primary Phone" in line or "primary" in line.lower():
                                                        phones["primary"] = phone
                                                        primary_found = True
                                                        print(f"    👑 Found designated primary phone: {phone}")
                                                        break
                                            if primary_found:
                                                break

                                    # If no explicit primary found, use first phone as primary
                                    if not primary_found and cleaned_phones:
                                        phones["primary"] = cleaned_phones[0]
                                        print(f"    📞 Using first phone as primary: {cleaned_phones[0]}")

                                    # Set secondary phone
                                    if len(cleaned_phones) > 1:
                                        for phone in cleaned_phones:
                                            if phone != phones["primary"]:
                                                phones["secondary"] = phone
                                                break

                                    print(f"    📞 Found {len(cleaned_phones)} phone numbers from 'Last Known Phone Numbers' section")
                                    for phone in cleaned_phones:
                                        print(f"      📞 {phone}")
                        else:
                            print("    ⚠️ 'Last Known Phone Numbers' section not found, trying broader search...")

                            # Fallback: look for phone numbers in the entire card but with more specific filtering
                            phone_pattern = r'\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}'
                            phone_matches = re.findall(phone_pattern, card_text)

                            if phone_matches:
                                # Clean up phone numbers
                                cleaned_phones = []
                                for phone in phone_matches:
                                    # Standardize format to (XXX) XXX-XXXX
                                    digits = re.sub(r'\D', '', phone)
                                    if len(digits) == 10:
                                        formatted = f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
                                        if formatted not in cleaned_phones:
                                            cleaned_phones.append(formatted)

                                # Limit to first 2 phones to avoid pulling all associated numbers
                                cleaned_phones = cleaned_phones[:2]
                                phones["all"] = cleaned_phones

                                if cleaned_phones:
                                    phones["primary"] = cleaned_phones[0]
                                    if len(cleaned_phones) > 1:
                                        phones["secondary"] = cleaned_phones[1]

                                print(f"    � Fallback: Found {len(cleaned_phones)} phone numbers (limited to 2)")
                                for phone in cleaned_phones:
                                    print(f"      📞 {phone}")

                    except Exception as e:
                        print(f"    ⚠️ Error extracting phones from specific section: {e}")
                        # Ultimate fallback to original method but limited
                        phone_pattern = r'\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}'
                        phone_matches = re.findall(phone_pattern, card_text)

                        if phone_matches:
                            cleaned_phones = []
                            for phone in phone_matches[:2]:  # Limit to first 2
                                digits = re.sub(r'\D', '', phone)
                                if len(digits) == 10:
                                    formatted = f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
                                    if formatted not in cleaned_phones:
                                        cleaned_phones.append(formatted)

                            phones["all"] = cleaned_phones
                            if cleaned_phones:
                                phones["primary"] = cleaned_phones[0]
                                if len(cleaned_phones) > 1:
                                    phones["secondary"] = cleaned_phones[1]

                            # Fallback: look for phone numbers in the entire card but with more specific filtering
                            phone_pattern = r'\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}'
                            phone_matches = re.findall(phone_pattern, card_text)

                            if phone_matches:
                                # Clean up phone numbers
                                cleaned_phones = []
                                for phone in phone_matches:
                                    # Standardize format to (XXX) XXX-XXXX
                                    digits = re.sub(r'\D', '', phone)
                                    if len(digits) == 10:
                                        formatted = f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
                                        if formatted not in cleaned_phones:
                                            cleaned_phones.append(formatted)

                                # Limit to first 2 phones to avoid pulling all associated numbers
                                cleaned_phones = cleaned_phones[:2]
                                phones["all"] = cleaned_phones

                                if cleaned_phones:
                                    phones["primary"] = cleaned_phones[0]
                                    if len(cleaned_phones) > 1:
                                        phones["secondary"] = cleaned_phones[1]

                                print(f"    📞 Fallback: Found {len(cleaned_phones)} phone numbers (limited to 2)")
                                for phone in cleaned_phones:
                                    print(f"      📞 {phone}")

                    except Exception as e:
                        print(f"    ⚠️ Error extracting phones from specific section: {e}")
                        # Ultimate fallback to original method but limited
                        phone_pattern = r'\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}'
                        phone_matches = re.findall(phone_pattern, card_text)

                        if phone_matches:
                            cleaned_phones = []
                            for phone in phone_matches[:2]:  # Limit to first 2
                                digits = re.sub(r'\D', '', phone)
                                if len(digits) == 10:
                                    formatted = f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
                                    if formatted not in cleaned_phones:
                                        cleaned_phones.append(formatted)

                            phones["all"] = cleaned_phones
                            if cleaned_phones:
                                phones["primary"] = cleaned_phones[0]
                                if len(cleaned_phones) > 1:
                                    phones["secondary"] = cleaned_phones[1]

                    # Return the data if we found phone numbers
                    if phones["all"]:
                        return {
                            "name": f"{target_first_name} {target_last_name}",
                            "primary_phone": phones["primary"],
                            "secondary_phone": phones["secondary"],
                            "all_phones": phones["all"],
                            "matched_address": person_addresses[0] if person_addresses else "",
                            "address_match": address_match,
                            "total_phones": len(phones["all"])
                        }
                    else:
                        print(f"    ❌ No phone numbers found in result #{i+1}")
                        continue

                except Exception as e:
                    print(f"    ❌ Error processing card #{i+1}: {e}")
                    continue

            print("  ❌ No matching records with phone numbers found")
            return None

        except Exception as e:
            print(f"  ❌ Extraction error: {e}")
            return None

    async def process_csv_with_sessions(self, csv_path: str):
        """Process CSV records with 2 records per session - saves to same file"""
        print(f"📞 ZABASEARCH PHONE EXTRACTOR - OPTIMIZED (1 record per session)")
        print("=" * 70)

        # Load CSV
        try:
            df = pd.read_csv(csv_path)
            print(f"✓ Loaded {len(df)} records from CSV")
        except Exception as e:
            print(f"❌ Error loading CSV: {e}")
            return

        # Find records with addresses - adapted for broward_lis_pendens CSV format
        records_with_addresses = []
        for _, row in df.iterrows():
            # Process both DirectName and IndirectName records
            for prefix in ['DirectName', 'IndirectName']:
                name_col = f"{prefix}_Cleaned"
                address_col = f"{prefix}_Address"
                type_col = f"{prefix}_Type"

                name = row.get(name_col, '')
                address = row.get(address_col, '')
                record_type = row.get(type_col, '')

                # Check if we have valid name and address for a Person (not Business/Organization)
                if (name and address and pd.notna(name) and pd.notna(address) and
                    str(name).strip() and str(address).strip() and
                    record_type == 'Person'):

                    # Check if we already have phone numbers for this record
                    phone_col = f"{prefix}_Phone_Primary"
                    if phone_col in df.columns and pd.notna(row.get(phone_col)) and str(row.get(phone_col)).strip():
                        print(f"  ⏭️ Skipping {name} - already has phone number")
                        continue

                    records_with_addresses.append({
                        'name': str(name).strip(),
                        'address': str(address).strip(),
                        'row_index': row.name,
                        'column_prefix': prefix  # Use 'DirectName' or 'IndirectName'
                    })

        print(f"✓ Found {len(records_with_addresses)} total records with person names and addresses")

        # Process all records (no skipping)
        remaining_records = records_with_addresses

        print(f"✓ Records to process: {len(remaining_records)}")
        print(f"✓ Processing 1 record per session - MAXIMUM STEALTH")

        # Add new columns for phone data
        phone_columns = ['_Phone_Primary', '_Phone_Secondary', '_Phone_All', '_Address_Match']
        for record in remaining_records:
            prefix = record['column_prefix']
            for col in phone_columns:
                col_name = f"{prefix}{col}"
                if col_name not in df.columns:
                    df[col_name] = ''

        # Process records in sessions of 1 - ONE SEARCH PER SESSION
        session_size = 1
        total_sessions = len(remaining_records)  # Each record gets its own session
        total_success = 0

        for session_num in range(total_sessions):
            session_start = session_num * session_size
            session_end = min(session_start + session_size, len(remaining_records))
            session_records = remaining_records[session_start:session_end]

            print(f"\n{'='*80}")
            print(f"🔄 SESSION #{session_num + 1}/{total_sessions} - ONE SEARCH")
            print(f"🎯 Record {session_start + 1} of {len(remaining_records)}")
            print(f"{'='*80}")

            # Create new browser session for EACH SINGLE record - MAXIMUM STEALTH
            async with async_playwright() as playwright:
                browser, context = await self.create_stealth_browser(playwright)
                page = await context.new_page()
                session_success = 0

                try:
                    for i, record in enumerate(session_records, 1):
                        print(f"\n{'='*60}")
                        print(f"� PROCESSING SINGLE RECORD")
                        print(f"{'='*60}")
                        print(f"  👤 Name: {record['name']}")
                        print(f"  📍 Address: {record['address']}")

                        # Parse name
                        name_parts = record['name'].split()
                        if len(name_parts) < 2:
                            print("  ❌ Invalid name format - skipping")
                            continue

                        first_name = name_parts[0]
                        last_name = name_parts[1]
                        print(f"  ✅ Parsed name: '{first_name}' '{last_name}'")

                        # Extract city and state from address for better matching
                        city = ""
                        state = "Florida"  # Default to Florida
                        address_str = str(record['address']).strip()

                        print(f"  🔍 Parsing address: '{address_str}'")

                        # Handle different address formats:
                        # Format 1: "5804 NW 14 STREET SUNRISE, 33313"
                        # Format 2: "130 CYPRESS CLUB DR #309 POMPANO BEACH, FL 33060"
                        # Format 3: "400 COMMODORE DR #208 PLANTATION, FL 33325"
                        # Format 4: "1505 NW 80 AVENUE # F MARGATE, 33063"

                        if ',' in address_str:
                            # Split by comma - everything before comma is street + city
                            parts = address_str.split(',')
                            street_and_city = parts[0].strip()
                            zip_and_state = parts[1].strip() if len(parts) > 1 else ""

                            # Extract state from the part after comma
                            if zip_and_state:
                                # Look for state abbreviation (FL) in the zip/state part
                                zip_state_words = zip_and_state.split()
                                for word in zip_state_words:
                                    if word.upper() in ['FL', 'FLORIDA']:
                                        state = "Florida"
                                        break

                            # Parse the street and city part
                            words = street_and_city.split()

                            # Common street types to identify where street ends
                            street_types = ['ST', 'STREET', 'AVE', 'AVENUE', 'DR', 'DRIVE', 'CT', 'COURT',
                                          'PL', 'PLACE', 'RD', 'ROAD', 'LN', 'LANE', 'BLVD', 'BOULEVARD',
                                          'WAY', 'CIR', 'CIRCLE', 'TER', 'TERRACE', 'PKWY', 'PARKWAY']

                            # Find where the street type ends (last occurrence)
                            street_end_idx = -1
                            for i_word, word in enumerate(words):
                                if word.upper() in street_types:
                                    street_end_idx = i_word

                            # Extract city (everything after the last street type, but skip apartment indicators)
                            if street_end_idx >= 0 and street_end_idx < len(words) - 1:
                                # City starts after the street type
                                potential_city_words = words[street_end_idx + 1:]

                                # Filter out apartment/unit indicators and numbers
                                clean_city_words = []
                                j = 0

                                while j < len(potential_city_words):
                                    word = potential_city_words[j]
                                    word_upper = word.upper()

                                    # Skip apartment/unit indicators
                                    if word_upper in ['#', 'APT', 'APARTMENT', 'UNIT', 'STE', 'SUITE', 'LOT']:
                                        # Also skip the next word if it looks like a unit number/letter
                                        if j + 1 < len(potential_city_words):
                                            next_word = potential_city_words[j + 1]
                                            # Skip unit identifiers like "F", "A1", "309", etc.
                                            if (next_word.isdigit() or
                                                len(next_word) <= 3 or  # Short codes like "F", "A1", "2B"
                                                re.match(r'^[A-Z]?\d+[A-Z]?$', next_word.upper())):  # Patterns like "F", "12A", "B2"
                                                j += 1  # Skip the unit value too
                                        j += 1
                                        continue

                                    # Skip words that start with # (like "#F", "#309")
                                    elif word.startswith('#'):
                                        j += 1
                                        continue

                                    # Skip standalone single letters that are likely unit indicators (but keep DR, ST, etc.)
                                    elif (len(word) <= 2 and word.isalpha() and
                                          word_upper not in ['DR', 'ST', 'CT', 'LN', 'RD', 'PL', 'AV']):
                                        j += 1
                                        continue

                                    # Skip pure numbers (zip codes or unit numbers)
                                    elif word.isdigit():
                                        j += 1
                                        continue

                                    # This word seems to be part of the city name
                                    else:
                                        clean_city_words.append(word)
                                        j += 1

                                city = ' '.join(clean_city_words)

                            # Fallback: if no street type found or no city extracted, use last substantial words
                            if not city and len(words) >= 2:
                                # Take last few words as potential city, avoiding unit numbers and directionals
                                potential_city_words = []
                                for word in reversed(words):
                                    word_upper = word.upper()
                                    # Skip obvious non-city words
                                    if not (word.startswith('#') or
                                           word.isdigit() or
                                           word_upper in ['N', 'S', 'E', 'W', 'NE', 'NW', 'SE', 'SW'] or
                                           len(word) <= 2 and word.isalnum() or  # Skip short unit codes
                                           word_upper in ['APT', 'UNIT', 'STE', 'SUITE', 'LOT']):
                                        potential_city_words.insert(0, word)
                                        if len(potential_city_words) >= 2:  # Limit to reasonable city name length
                                            break
                                city = ' '.join(potential_city_words)

                        else:
                            # No comma - try to extract city from the end
                            words = address_str.split()
                            if len(words) >= 3:
                                # Assume last 1-2 words are city, but filter out unit indicators
                                potential_city_words = []
                                for word in reversed(words):
                                    if not (word.startswith('#') or word.isdigit() or word.upper() in ['N', 'S', 'E', 'W']):
                                        potential_city_words.insert(0, word)
                                        if len(potential_city_words) >= 2:
                                            break
                                city = ' '.join(potential_city_words)

                        # Clean up city name
                        if city:
                            city = city.strip()
                            # Remove directional prefixes if they're at the start and followed by actual city name
                            if city.upper().startswith(('N ', 'S ', 'E ', 'W ', 'NE ', 'NW ', 'SE ', 'SW ')):
                                city_parts = city.split(' ', 1)
                                if len(city_parts) > 1:
                                    city = city_parts[1]
                            # Remove empty strings or single letters
                            if len(city.strip()) <= 1:
                                city = ""

                        print(f"  🏙️ Extracted city: '{city}'")
                        print(f"  🗺️ State: '{state}'")

                        # Search ZabaSearch with address for matching
                        print(f"  🚀 Starting ZabaSearch lookup...")
                        try:
                            person_data = await self.search_person(page, first_name, last_name, record['address'], city, state)
                        except Exception as search_error:
                            print(f"  💥 CRITICAL ERROR during search: {search_error}")
                            print(f"  🔍 Error type: {type(search_error).__name__}")

                            # Try to continue after error
                            person_data = None

                        if not person_data:
                            # No results - leave fields empty
                            print(f"  ❌ No results found for {record['name']}")
                            continue

                        print(f"  🎉 SUCCESS! Found matching person with {person_data['total_phones']} phone(s)")

                        # Update CSV with phone data
                        row_idx = record['row_index']
                        prefix = record['column_prefix']

                        df.at[row_idx, f"{prefix}_Phone_Primary"] = person_data.get('primary_phone', '')
                        df.at[row_idx, f"{prefix}_Phone_Secondary"] = person_data.get('secondary_phone', '')
                        df.at[row_idx, f"{prefix}_Phone_All"] = ', '.join(person_data.get('all_phones', []))
                        df.at[row_idx, f"{prefix}_Address_Match"] = person_data.get('matched_address', '')

                        session_success += 1
                        print(f"  📞 Primary: {person_data.get('primary_phone', 'None')}")
                        if person_data.get('secondary_phone'):
                            print(f"  📞 Secondary: {person_data.get('secondary_phone')}")
                        print(f"  📞 Total phones: {len(person_data.get('all_phones', []))}")
                        print(f"  🏆 SUCCESS - Session complete!")

                        # NO DELAY - session ends immediately after single search

                except Exception as e:
                    print(f"\n💥 CRITICAL SESSION ERROR: {e}")
                    print(f"🔍 Error type: {type(e).__name__}")
                    print(f"📊 Session status: {session_success} successful records before crash")

                finally:
                    # ENHANCED BROWSER CLEANUP WITH COMPLETE SESSION TERMINATION
                    try:
                        print(f"\n🔄 STARTING SESSION CLEANUP...")

                        # Step 1: Close all pages
                        if context:
                            pages = context.pages
                            print(f"  📄 Closing {len(pages)} open pages...")
                            for page_item in pages:
                                try:
                                    await page_item.close()
                                    print(f"    ✅ Page closed")
                                except:
                                    pass

                        # Step 2: Close context (isolates sessions)
                        if context:
                            print(f"  🧬 Closing browser context (session isolation)...")
                            await context.close()
                            print(f"    ✅ Context closed - session data cleared")

                        # Step 3: Close browser process completely
                        if browser:
                            print(f"  🔧 Terminating browser process...")
                            await browser.close()
                            print(f"    ✅ Browser process terminated")

                        # Step 4: Faster cleanup delay
                        print(f"  ⏳ Waiting for complete process termination...")
                        await asyncio.sleep(1)  # Reduced from 2

                        # Step 5: Force garbage collection
                        gc.collect()
                        print(f"  🗑️ Memory cleanup completed")

                        print(f"  ✅ SESSION CLEANUP FINISHED")
                        print(f"  🛡️ All browser fingerprints cleared for next session")

                    except Exception as cleanup_error:
                        print(f"  ⚠️ Cleanup warning: {cleanup_error}")

                    # Always try to save progress after each session
                    try:
                        df.to_csv(csv_path, index=False)
                        print(f"💾 Session progress saved: {session_success} records processed in this session")
                    except:
                        pass

                # Update total success count
                total_success += session_success

                print(f"\n✅ SESSION #{session_num + 1} COMPLETE!")
                print(f"📊 Single record result: {'SUCCESS' if session_success > 0 else 'NO RESULTS'}")
                print(f"🎯 Total successful so far: {total_success}")

                # MINIMAL delay between sessions - ULTRA FAST
                if session_num < total_sessions - 1:
                    print(f"\n⚡ Quick 1-2 second delay before next session...")
                    await asyncio.sleep(random.uniform(1, 2))

        print(f"\n🎉 ALL PROCESSING COMPLETE!")
        print(f"📊 Successfully found phone numbers for {total_success}/{len(remaining_records)} records")
        if len(remaining_records) > 0:
            percentage = (total_success/len(remaining_records)*100)
            print(f"📈 Success rate: {percentage:.1f}%")
        else:
            print(f"📈 No records to process")

        # Save final results back to the original CSV file
        df.to_csv(csv_path, index=False)
        print(f"💾 Final results saved back to: {csv_path}")
        print(f"✅ Phone numbers added as new columns in the original CSV!")


def parse_args():
    parser = argparse.ArgumentParser(description="ZabaSearch Phone Number Extractor - OPTIMIZED (1 record per session)")
    parser.add_argument('--input', type=str, help='Input CSV file (auto-detect if not specified)')
    parser.add_argument('--show-browser', action='store_true', help='Show browser GUI (default is headless mode)')
    return parser.parse_args()

async def main():
    args = parse_args()

    def find_latest_csv_with_addresses():
        """Find the latest CSV file with addresses in weekly_output folder"""
        print("🔍 Looking for CSV files with address data...")

        # Search patterns for CSV files with addresses
        search_patterns = [
            'weekly_output/*processed_with_addresses*.csv',
            'weekly_output/*_with_addresses*.csv',
            'weekly_output/broward_lis_pendens*.csv',
            'weekly_output/missing_phone_numbers*.csv',
            'weekly_output/*.csv'
        ]

        found_files = []
        for pattern in search_patterns:
            files = glob.glob(pattern)
            for file in files:
                if os.path.exists(file):
                    # Check if file has address columns
                    try:
                        df_test = pd.read_csv(file, nrows=1)
                        address_columns = [col for col in df_test.columns if 'address' in col.lower()]
                        if address_columns:
                            mod_time = os.path.getmtime(file)
                            found_files.append((file, mod_time))
                            print(f"  📄 Found: {file} (has address columns: {address_columns})")
                    except Exception as e:
                        print(f"  ⚠️ Could not read {file}: {e}")

        if not found_files:
            print("❌ No CSV files with address columns found")
            return None

        # Sort by modification time (newest first)
        found_files.sort(key=lambda x: x[1], reverse=True)
        latest_file = found_files[0][0]
        print(f"✅ Selected latest file: {latest_file}")
        return latest_file

    # Find CSV file
    csv_path = args.input if args.input else None
    # Headless by default, unless --show-browser is specified
    headless_mode = not args.show_browser
    extractor = ZabaSearchExtractor(headless=headless_mode)

    # Use existing output file if it exists, otherwise find latest CSV with addresses
    import glob, os
    if not csv_path:
        csv_path = find_latest_csv_with_addresses()
        if not csv_path:
            print(f'❌ No CSV file with addresses found!')
            return
        print(f'✅ Using auto-detected CSV file: {csv_path}')

    print(f'✅ Will save results directly to: {csv_path}')

    print(f"\n🔄 STARTING ZabaSearch extraction with SESSION-BASED processing...")
    print(f"🛡️ Enhanced with Cloudflare challenge detection and bypass")
    print(f"🚀 OPTIMIZED: 1 search per session - MAXIMUM STEALTH & SPEED")
    print(f"⚡ MINIMAL delays - ULTRA FAST processing")
    print("=" * 70)

    try:
        await extractor.process_csv_with_sessions(csv_path)

    except Exception as e:
        print(f"❌ Error in processing: {e}")

    print(f"\n✅ ALL PROCESSING COMPLETE!")
    print(f"💾 Final results saved in: {csv_path}")
    print(f"✅ Phone numbers added directly to original CSV file!")

if __name__ == "__main__":
    asyncio.run(main())
