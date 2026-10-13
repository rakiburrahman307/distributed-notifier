import { describe, it } from 'node:test';
import assert from 'node:assert';
import { TemplateEngine } from '../src/engines/template-engine.js';

describe('Advanced Template Engine (Nested Paths, Conditionals, i18n)', () => {
  it('should interpolate nested object paths and arrays', () => {
    const engine = new TemplateEngine();
    engine.register({
      id: 'order_receipt',
      title: 'Order #{{order.id}} Confirmed',
      body: 'Hello {{user.profile.name}}, your {{order.items.0.title}} is on the way to {{user.address.city}}!',
    });

    const rendered = engine.render('order_receipt', {
      user: {
        profile: { name: 'Rakibur' },
        address: { city: 'Dhaka' },
      },
      order: {
        id: '9921',
        items: [{ title: 'Mechanical Keyboard' }],
      },
    });

    assert.strictEqual(rendered.title, 'Order #9921 Confirmed');
    assert.strictEqual(
      rendered.body,
      'Hello Rakibur, your Mechanical Keyboard is on the way to Dhaka!'
    );
  });

  it('should evaluate conditional blocks with {{#if}} and {{else}}', () => {
    const engine = new TemplateEngine();
    engine.register({
      id: 'membership_status',
      title: '{{#if isPremium}}Premium Account{{else}}Free Account{{/if}}',
      body: 'Welcome! {{#if discount}}Special discount: {{discount}}% off!{{/if}} Enjoy.',
    });

    // Test truthy condition
    const resPremium = engine.render('membership_status', { isPremium: true, discount: 25 });
    assert.strictEqual(resPremium.title, 'Premium Account');
    assert.strictEqual(resPremium.body, 'Welcome! Special discount: 25% off! Enjoy.');

    // Test falsy condition
    const resFree = engine.render('membership_status', { isPremium: false });
    assert.strictEqual(resFree.title, 'Free Account');
    assert.strictEqual(resFree.body, 'Welcome!  Enjoy.');
  });

  it('should support i18n multi-language templates with fallback', () => {
    const engine = new TemplateEngine();
    engine.register({
      id: 'welcome_user',
      title: 'Welcome {{name}}',
      body: 'Thanks for joining our service.',
      defaultLocale: 'en',
      locales: {
        bn: {
          title: 'স্বাগতম {{name}}',
          body: 'আমাদের সার্ভিসে যোগদানের জন্য ধন্যবাদ।',
        },
        fr: {
          title: 'Bienvenue {{name}}',
          body: 'Merci de rejoindre notre service.',
        },
      },
    });

    // English (default)
    const enRes = engine.render('welcome_user', { name: 'Rahman' });
    assert.strictEqual(enRes.title, 'Welcome Rahman');
    assert.strictEqual(enRes.resolvedLocale, 'en');

    // Bengali
    const bnRes = engine.render('welcome_user', { name: 'রাকিবুর' }, 'bn');
    assert.strictEqual(bnRes.title, 'স্বাগতম রাকিবুর');
    assert.strictEqual(bnRes.body, 'আমাদের সার্ভিসে যোগদানের জন্য ধন্যবাদ।');
    assert.strictEqual(bnRes.resolvedLocale, 'bn');

    // Unknown locale falls back to default
    const deRes = engine.render('welcome_user', { name: 'Hans' }, 'de');
    assert.strictEqual(deRes.title, 'Welcome Hans');
    assert.strictEqual(deRes.resolvedLocale, 'en');
  });

  it('should escape HTML in {{var}} and allow raw in {{{var}}}', () => {
    const engine = new TemplateEngine();
    engine.register({
      id: 'xss_test',
      title: 'Escaped: {{input}}',
      body: 'Raw: {{{input}}}',
    });

    const rendered = engine.render('xss_test', { input: '<script>alert("xss")</script>' });
    assert.strictEqual(rendered.title, 'Escaped: &lt;script&gt;alert(&quot;xss&quot;)&lt;/script&gt;');
    assert.strictEqual(rendered.body, 'Raw: <script>alert("xss")</script>');
  });
});
