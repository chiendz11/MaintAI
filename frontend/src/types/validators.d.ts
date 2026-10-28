declare module "@maintai/validators" {
  export const alertPageValidator: (value: unknown) => boolean;
  export const alertDetailValidator: (value: unknown) => boolean;
  export const transitionValidator: (value: unknown) => boolean;
  export const feedbackValidator: (value: unknown) => boolean;
  export const liveValidator: (value: unknown) => boolean;
  export const eventValidator: (value: unknown) => boolean;
  export const pageValidator: (value: unknown) => boolean;
  export const detailValidator: (value: unknown) => boolean;
}
