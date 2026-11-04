declare module "@maintai/validators" {
  export const historyPageValidator: (value: unknown) => boolean;
  export const rawHistoryPageValidator: (value: unknown) => boolean;
  export const chartValidator: (value: unknown) => boolean;
  export const exportJobValidator: (value: unknown) => boolean;
  export const alertPageValidator: (value: unknown) => boolean;
  export const alertDetailValidator: (value: unknown) => boolean;
  export const transitionValidator: (value: unknown) => boolean;
  export const feedbackValidator: (value: unknown) => boolean;
  export const liveValidator: (value: unknown) => boolean;
  export const eventValidator: (value: unknown) => boolean;
  export const pageValidator: (value: unknown) => boolean;
  export const detailValidator: (value: unknown) => boolean;
}
