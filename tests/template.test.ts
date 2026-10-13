import { describe, it } from 'node:test';
import assert from 'node:assert';
import { TemplateEngine } from '../src/engines/template-engine.js';

describe('Template Engine', () => {
  it('should interpolate variables into registered template', () => {
    const engine = new TemplateEngine();
    engine.register({
      id: 'ORDER_STATUS',
      title: 'Order #{{orderId}} Confirmed',
      body: 'Hi {{customerName}}, your total is ${{totalAmount}}.',
      defaultChannels: ['push', 'sms'],
    });

    const rendered = engine.render('ORDER_STATUS', {
      orderId: '4920',
      customerName: 'Alice',
      totalAmount: 45,
    });

    assert.strictEqual(rendered.title, 'Order #4920 Confirmed');
    assert.strictEqual(rendered.body, 'Hi Alice, your total is $45.');
    assert.deepStrictEqual(rendered.defaultChannels, ['push', 'sms']);
  });

  it('should throw an error if template ID does not exist', () => {
    const engine = new TemplateEngine();
    assert.throws(() => {
      engine.render('NON_EXISTENT_TEMPLATE');
    }, /not found/);
  });
});
