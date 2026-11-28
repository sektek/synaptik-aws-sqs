import { Event } from '@sektek/synaptik';
import { ProviderComponent } from '@sektek/utility-belt';

/**
 * Options for the SqsEventProcessor.
 */
export type SqsEventProcessorOptions = {
  /**
   * Converts the body of an SQS message into an Event. May throw or reject
   * when the body cannot be converted. Defaults to parsing the body as a JSON
   * encoded Event.
   */
  deserializer?: ProviderComponent<Event, string>;
};
