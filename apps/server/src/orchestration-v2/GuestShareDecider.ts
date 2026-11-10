import {
  type GuestMessageAttribution,
  type GuestMessageId,
  type OrchestrationV2AppThread,
  type OrchestrationV2ServerCommand,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";

export type GuestShareCommand = Extract<
  OrchestrationV2ServerCommand,
  {
    readonly type:
      | "thread.guest-share.open"
      | "thread.guest-share.redeem"
      | "thread.guest-share.close"
      | "thread.guest-message.submit"
      | "thread.guest-message.dismiss";
  }
>;

/** Failures a guest may be told about. Any other dispatch failure reaches them as "rejected". */
export const GUEST_FAILURE = {
  inactive: "This guest link is no longer active.",
  used: "This guest link was already used.",
  busy: "Wait for the owner to answer your earlier messages.",
  filesBusy: "Wait for the owner to answer your earlier messages before you attach more files.",
} as const;

export interface DecideGuestShareInput {
  readonly thread: OrchestrationV2AppThread;
  readonly command: GuestShareCommand;
  readonly now: DateTime.Utc;
}
export interface SendGuestMessageInput {
  readonly thread: OrchestrationV2AppThread;
  readonly guestMessageId: GuestMessageId;
  readonly now: DateTime.Utc;
}
export type CancelGuest = Pick<GuestMessageAttribution, "guestMessageId" | "shareId">;
