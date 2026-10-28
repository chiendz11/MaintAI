declare module "@maintai/validators" {
  export const liveValidator: (value: unknown) => boolean;
  export const eventValidator: (value: unknown) => boolean;
  export const pageValidator: (value: unknown) => boolean;
  export const detailValidator: (value: unknown) => boolean;
}
